"""
stock_pipeline_dag.py — M3/M4 Orchestration
============================================
Real-Time US Stocks Data Pipeline

Runs the full batch/orchestration leg of the pipeline end to end:

    1. check_kafka_health      Confirms the Kafka cluster (M1) is up and the
                                expected topics exist, by execing into
                                `kafka-1` and listing topics. Fails fast if
                                M1 isn't running, instead of Spark failing
                                later with a confusing connection error.

    2. check_postgres_health   Confirms the analytics Postgres (M3 sink) is
                                reachable via a plain `SELECT 1`, before we
                                spend minutes running Spark against it.

    3. spark_analytics_job     spark-submit analytics_job.py inside the
                                already-running `stocks-spark` container.
                                Reads clean Parquet from MinIO, writes 9
                                insight tables to Postgres via JDBC.

    4. load_post_analytics_sql Runs post_analytics.sql (psql, not plain SQL —
                                it uses \\echo meta-commands) inside the
                                `stocks-postgres` container to restore the
                                indexes/views that Spark's JDBC
                                mode="overwrite" drops on every run.

    5. validate_data           Row-count sanity check on every table
                                analytics_job.py is supposed to have written.
                                Fails the DAG run if any table is empty.

DESIGN NOTE — why "docker exec" instead of DockerOperator / SparkSubmitOperator
--------------------------------------------------------------------------------
`stocks-spark` and `stocks-postgres` are already long-running services in
docker-compose.yml with all the right JARs, env vars, and volumes baked in.
Rather than have Airflow spin up *new* containers (which, over the Docker
socket from inside another container, needs host-absolute volume paths and
duplicates config), each task execs into the existing service container —
exactly what a human would type at the terminal, just automated. Airflow's
job here is orchestration/scheduling/retries, not re-implementing Spark.

This does mean airflow-webserver/airflow-scheduler need /var/run/docker.sock
mounted (see docker-compose.yml) and the `docker` python package (see
airflow/requirements-airflow.txt).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import docker
from docker.errors import NotFound

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

SPARK_CONTAINER = "stocks-spark"
POSTGRES_CONTAINER = "stocks-postgres"
KAFKA_CONTAINER = "kafka-1"
KAFKA_BOOTSTRAP = "kafka-1:29092"
POSTGRES_CONN_ID = "stocks_postgres"  # auto-registered via AIRFLOW_CONN_STOCKS_POSTGRES

# Topics kafka-init creates on stack startup — see docker-compose.yml
EXPECTED_TOPICS = ["us-stocks-raw", "us-stocks-dead-letter"]

# Every table analytics_job.py writes on a normal run (streaming_window_summary
# is intentionally excluded — it's conditional on live streaming data existing).
EXPECTED_TABLES = [
    "stock_summary",
    "price_volatility",
    "monthly_performance",
    "yearly_performance",
    "top_performers",
    "volume_leaders",
    "dividend_analysis",
    "stochastic_signals",
    "daily_market_breadth",
]

SPARK_SUBMIT_CMD = """
spark-submit \
  --master local[4] \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.access.key=$MINIO_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=$MINIO_SECRET_KEY \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.sql.files.ignoreMissingFiles=true \
  /opt/spark/work-dir/analytics_job.py
""".strip()

POST_ANALYTICS_CMD = (
    "psql -U $POSTGRES_USER -d $POSTGRES_DB -f /opt/sql/post_analytics.sql"
)


# ─────────────────────────────────────────────────────────────────────────────
# Shared helper — run a command inside an already-running container
# ─────────────────────────────────────────────────────────────────────────────
def _exec_in_container(container_name: str, shell_command: str) -> str:
    """Run `shell_command` via `bash -c` inside `container_name`, return its
    combined stdout/stderr as text, and raise AirflowException (→ Airflow
    retry) if it exits non-zero."""
    client = docker.from_env()
    try:
        container = client.containers.get(container_name)
    except NotFound as exc:
        raise AirflowException(
            f"Container '{container_name}' is not running. "
            f"Is the full docker-compose stack up? (docker compose ps)"
        ) from exc

    log.info("Executing in %s:\n%s", container_name, shell_command)
    exit_code, output = container.exec_run(["bash", "-c", shell_command], demux=False)
    text = output.decode(errors="replace")
    log.info(text)

    if exit_code != 0:
        raise AirflowException(
            f"Command in container '{container_name}' failed with exit code {exit_code}"
        )
    return text


def check_kafka_health(**_context) -> None:
    """M1 pre-flight check: brokers reachable + expected topics exist.
    Fails fast here instead of Spark timing out later with a vaguer error."""
    output = _exec_in_container(
        KAFKA_CONTAINER,
        f"kafka-topics --bootstrap-server {KAFKA_BOOTSTRAP} --list",
    )
    existing_topics = {line.strip() for line in output.splitlines() if line.strip()}
    missing = [t for t in EXPECTED_TOPICS if t not in existing_topics]
    if missing:
        raise AirflowException(
            f"Kafka is up but missing expected topic(s): {missing}. "
            f"Found: {sorted(existing_topics)}. Has kafka-init run yet?"
        )
    log.info("Kafka healthy. Expected topics present: %s", EXPECTED_TOPICS)


def check_postgres_health(**_context) -> None:
    """M3 pre-flight check: analytics Postgres is reachable before we spend
    minutes running the Spark job against it."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    result = hook.get_first("SELECT 1;")
    if not result or result[0] != 1:
        raise AirflowException("Postgres health check returned an unexpected result.")
    log.info("Postgres healthy (SELECT 1 succeeded).")


def run_spark_analytics(**_context) -> None:
    _exec_in_container(SPARK_CONTAINER, SPARK_SUBMIT_CMD)


def run_post_analytics_sql(**_context) -> None:
    _exec_in_container(POSTGRES_CONTAINER, POST_ANALYTICS_CMD)


def validate_data(**_context) -> None:
    """Row-count sanity check. Fails the task (and the DAG run) if any
    expected table is missing or empty — this is what should trip an
    on-call alert / SLA miss, not a silent green run."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    failures: list[str] = []

    for table in EXPECTED_TABLES:
        try:
            row = hook.get_first(f"SELECT COUNT(*) FROM {table};")
            count = row[0] if row else 0
        except Exception as exc:  # table missing, connection issue, etc.
            failures.append(f"{table}: query failed ({exc})")
            continue

        log.info("  %-25s %d rows", table, count)
        if count == 0:
            failures.append(f"{table}: 0 rows")

    if failures:
        raise AirflowException(
            "Validation failed for the following tables:\n" + "\n".join(failures)
        )

    log.info("Validation passed: all %d tables have data.", len(EXPECTED_TABLES))


# ─────────────────────────────────────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # SLA: if the whole run isn't done in 45 min, Airflow marks it as an SLA
    # miss (visible in the UI / sendable to Slack/email) without failing the
    # run outright — the numbers here are a starting point that the team
    # should tune once we know real Spark job durations on your hardware.
    "sla": timedelta(minutes=45),
}

with DAG(
    dag_id="stock_analytics_pipeline",
    description="Kafka health -> Postgres health -> SparkSQL analytics -> Postgres load/index -> validation",
    default_args=default_args,
    schedule="0 6 * * *",       # daily 06:00 UTC — change to suit your data refresh cadence
    start_date=datetime(2026, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
    max_active_runs=1,          # never let two analytics runs overlap
    tags=["m1", "m3", "kafka", "spark", "postgres"],
) as dag:

    check_kafka = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka_health,
    )

    check_postgres = PythonOperator(
        task_id="check_postgres_health",
        python_callable=check_postgres_health,
    )

    spark_analytics_job = PythonOperator(
        task_id="spark_analytics_job",
        python_callable=run_spark_analytics,
    )

    load_post_analytics_sql = PythonOperator(
        task_id="load_post_analytics_sql",
        python_callable=run_post_analytics_sql,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    [check_kafka, check_postgres] >> spark_analytics_job >> load_post_analytics_sql >> validate
