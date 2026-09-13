# airflow/Dockerfile
# =============================================================================
# Custom Airflow image for the Real-Time US Stocks Data Pipeline.
#
# Adds:
#   • apache-airflow-providers-postgres  → SQLExecuteQueryOperator / PostgresHook
#     for the "load post_analytics.sql" and "validate" tasks.
#   • docker (python SDK)                → lets the DAG exec spark-submit
#     INSIDE the already-running `stocks-spark` container over the Docker
#     socket, instead of spinning up a second Spark container. Airflow
#     itself never touches Spark/S3A directly.
# =============================================================================
FROM apache/airflow:2.10.4-python3.11

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt
