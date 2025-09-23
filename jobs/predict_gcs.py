THIS SHOULD BE A LINTER ERRORimport os
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler


def get_env(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    return value


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


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
    app_name = get_env("APP_NAME", "Loan Prediction Pipeline")
    pipeline_path = get_env("PIPELINE_PATH", "gs://default-prediction-bucket/loan_data/pipeline/loan_rf_pipeline")
    input_csv_path = get_env("INPUT_CSV_PATH", "gs://default-prediction-bucket/loan_data/credit_risk_dataset.csv")
    output_predictions_path = get_env("OUTPUT_PREDICTIONS_PATH", "gs://default-prediction-bucket/loan_data/predictions/full_output")
    output_mode = get_env("OUTPUT_MODE", "overwrite")

    spark = build_spark(app_name)
    try:
        print(f"Loading pipeline from: {pipeline_path}")
        pipeline_model = PipelineModel.load(pipeline_path)
        print("Pipeline loaded successfully.")

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

        # Align additional assembler inputs discovered from the pipeline
        df = cast_and_fill_for_assembler_inputs(df, pipeline_model)

        print("Running pipeline transform…")
        predictions = pipeline_model.transform(df)

        # Show sample
        sample_cols = [c for c in ["person_age", "loan_amnt", "prediction", "probability"] if c in predictions.columns]
        if sample_cols:
            predictions.select(*sample_cols).show(10, truncate=False)

        print(f"Writing predictions to: {output_predictions_path} (mode={output_mode})")
        predictions.write.mode(output_mode).parquet(output_predictions_path)
        print("Predictions saved.")
    finally:
        spark.stop()
        print("Spark session stopped.")


if __name__ == "__main__":
    main()

