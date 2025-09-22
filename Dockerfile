FROM python:3.11-slim

# System dependencies (Java for Spark)
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirement files first for layer caching
COPY requirements_streamlit_cloud.txt /app/requirements_streamlit_cloud.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements_streamlit_cloud.txt

# Copy application code
COPY . /app

# Environment (override at runtime as needed)
ENV SPARK_MASTER=local[*]
ENV APP_NAME=loan-default-app
ENV MODEL_DIR=model

EXPOSE 8501

CMD ["bash", "-lc", "streamlit run streamlit_app.py --server.port=8501 --server.address=0.0.0.0"]

