import os
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array

from google.cloud import bigquery
from dotenv import load_dotenv


def get_env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    return value.strip() if value else default


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.37.0")
        .getOrCreate()
    )


def get_bq_table_id(dataset: str = "loan_ds", table: str = "loan_predictions") -> str:
    client = bigquery.Client()
    project_id = client.project
    return f"{project_id}.{dataset}.{table}"


def ensure_bq_dataset_and_table(df: DataFrame, dataset: str = "loan_ds", table: str = "loan_predictions") -> str:
    client = bigquery.Client()
    project_id = client.project
    dataset_id = f"{project_id}.{dataset}"

    # Ensure dataset exists
    try:
        client.get_dataset(dataset_id)
        print(f"✅ Dataset exists: {dataset_id}")
    except Exception:
        print(f"⚠️ Dataset not found. Creating: {dataset_id}")
        client.create_dataset(bigquery.Dataset(dataset_id))

    table_id = f"{dataset_id}.{table}"

    # Auto-generate schema from Spark DataFrame
    schema = []
    for field in df.schema.fields:
        if isinstance(field.dataType, (T.IntegerType, T.LongType, T.FloatType, T.DoubleType, T.DecimalType)):
            field_type = "FLOAT"
        else:
            field_type = "STRING"
        schema.append(bigquery.SchemaField(field.name, field_type))

    # Ensure table exists
    try:
        client.get_table(table_id)
        print(f"✅ Table exists: {table_id}")
    except Exception:
        print(f"⚠️ Table not found. Creating: {table_id}")
        table_obj = bigquery.Table(table_id, schema=schema)
        client.create_table(table_obj)
        print(f"✅ Table created: {table_id}")

    return table_id


def cast_numeric_columns_safely(df: DataFrame, numeric_candidate_cols: List[str]) -> DataFrame:
    for col_name in numeric_candidate_cols:
        if col_name not in df.columns:
            continue
        dtype = df.schema[col_name].dataType
        if isinstance(dtype, T.StringType):
            df = df.withColumn(
                col_name,
                F.when(F.trim(F.col(col_name)).isin("", "NaN", "nan", "null"), None)
                 .otherwise(F.col(col_name))
                 .cast(T.DoubleType()),
            )
        else:
            df = df.withColumn(col_name, F.col(col_name).cast(T.DoubleType()))
    return df


def fill_nulls_by_type(df: DataFrame) -> DataFrame:
    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, T.NumericType)]
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, T.StringType)]
    if numeric_cols:
        df = df.fillna(0, subset=numeric_cols)
    if string_cols:
        df = df.fillna("Unknown", subset=string_cols)
    return df


def cast_and_fill_for_assembler_inputs(df: DataFrame, pipeline_model: PipelineModel) -> DataFrame:
    assembler_inputs: List[str] = []
    for stage in pipeline_model.stages:
        if isinstance(stage, VectorAssembler):
            assembler_inputs.extend(stage.getInputCols())

    for col_name in assembler_inputs:
        if col_name in df.columns:
            dtype = df.schema[col_name].dataType
            # Only cast scalar columns; skip vectors or structs
            if not isinstance(dtype, (T.ArrayType, T.StructType)):
                if isinstance(dtype, T.StringType):
                    df = df.withColumn(
                        col_name,
                        F.when(F.trim(F.col(col_name)).isin("", "NaN", "nan", "null"), None)
                         .otherwise(F.col(col_name))
                         .cast(T.DoubleType()),
                    )
                else:
                    df = df.withColumn(col_name, F.col(col_name).cast(T.DoubleType()))

    # Fill nulls again now that assembler inputs are aligned
    df = fill_nulls_by_type(df)
    return df


def main() -> None:
    # Load .env if present
    load_dotenv(".env")

    app_name = get_env("APP_NAME", "Loan Prediction Pipeline")
    pipeline_path = get_env("PIPELINE_PATH", "gs://default-prediction-bucket/loan_data/pipeline/loan_rf_pipeline")
    input_csv_path = get_env("INPUT_CSV_PATH", "gs://default-prediction-bucket/loan_data/credit_risk_dataset.csv")

    spark = build_spark(app_name)
    try:
        print(f"Loading input CSV from: {input_csv_path}")
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(input_csv_path)
        )
        print(f"Input data loaded. Rows: {df.count()} Columns: {len(df.columns)}")

        # Candidate numeric fields commonly assembled by typical loan pipelines
        numeric_candidate_cols = [
            "person_age",
            "person_income",
            "person_emp_length",
            "loan_amnt",
            "loan_int_rate",
            "loan_percent_income",
            "cb_person_cred_hist_length",
        ]

        # Cast candidates to double first, then generic fill by type
        df = cast_numeric_columns_safely(df, numeric_candidate_cols)
        df = fill_nulls_by_type(df)

        print(f"Loading pipeline from: {pipeline_path}")
        pipeline_model = PipelineModel.load(pipeline_path)
        df = cast_and_fill_for_assembler_inputs(df, pipeline_model)

        print("Running pipeline transform…")
        predictions = pipeline_model.transform(df)

        # Expand probability vector and compute risk score
        if "probability" in predictions.columns:
            predictions = predictions.withColumn("probability_array", vector_to_array("probability"))
            predictions = predictions \
                .withColumn("prob_class_0", F.col("probability_array")[0]) \
                .withColumn("prob_class_1", F.col("probability_array")[1]) \
                .withColumn("risk_score", F.col("prob_class_1") * 100)

        # Select columns to persist to BigQuery
        bq_columns = [
            "person_age",
            "person_income",
            "person_emp_length",
            "loan_amnt",
            "loan_int_rate",
            "loan_percent_income",
            "cb_person_cred_hist_length",
            "prediction",
            "prob_class_0",
            "prob_class_1",
            "risk_score",
        ]
        bq_columns = [c for c in bq_columns if c in predictions.columns]
        predictions_to_bq = predictions.select(*bq_columns)

        # Ensure BQ dataset/table then append
        dataset = get_env("BQ_DATASET", "loan_ds")
        table = get_env("BQ_TABLE", "loan_predictions")
        bq_table_id = ensure_bq_dataset_and_table(predictions_to_bq, dataset=dataset, table=table)
        print(f"✅ Writing to BigQuery Table: {bq_table_id}")

        predictions_to_bq.write \
            .format("bigquery") \
            .option("table", bq_table_id) \
            .mode("append") \
            .save()
        print(f"✅ Predictions saved to BigQuery table: {bq_table_id}")
    finally:
        spark.stop()
        print("Spark session stopped.")


if __name__ == "__main__":
    main()

