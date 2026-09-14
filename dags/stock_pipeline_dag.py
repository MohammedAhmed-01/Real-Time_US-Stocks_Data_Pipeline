"""
stock_pipeline_dag.py — Full Pipeline Orchestration (M1 -> M2 -> M3 -> M4)
============================================================================
Real-Time US Stocks Data Pipeline

Runs the ENTIRE pipeline end to end, in the order the data actually has to
flow through the system:

    Kafka (M1)  -->  Spark Streaming (M2)  -->  MinIO (storage)
                 -->  SparkSQL Analytics (M3)  -->  PostgreSQL (M3 sink)

Concretely, the DAG tasks are:

    1. check_kafka_health       Confirms the Kafka cluster (M1) is up and the
                                 expected topics exist, by execing into
                                 `kafka-1` and listing topics. Fails fast if
                                 M1 isn't running, instead of Spark failing
                                 later with a confusing connection error.

    2. check_postgres_health    Confirms the analytics Postgres (M3 sink) is
                                 reachable via a plain `SELECT 1`, before we
                                 spend minutes running Spark against it.

    3. run_spark_streaming_job  Runs streaming_job.py (M2) inside the
                                 already-running `stocks-spark` container.
                                 This is the step that actually reads from
                                 Kafka, validates events, and writes clean +
                                 windowed Parquet into MinIO.

                                 streaming_job.py is a Structured Streaming
                                 job and blocks forever (awaitAnyTermination).
                                 Since this DAG's job is a scheduled BATCH run
                                 (not an always-on service), the job is
                                 wrapped in `timeout <N>s` so it drains Kafka
                                 into MinIO for a bounded window and then
                                 stops cleanly. Exit codes 124/143 (timeout's
                                 own SIGTERM) are treated as success, not
                                 failure — see STREAMING_RUN_SECONDS below to
                                 tune the window.

    4. check_minio_data         Confirms streaming_job.py actually produced
                                 clean Parquet in MinIO (s3a://stocks/clean)
                                 before we hand off to Spark analytics. Execs
                                 into the `stocks-minio` container (same
                                 image as minio-init, so `mc` is already
                                 there) and lists the clean/ prefix. Fails
                                 the DAG early — with a clear message — if
                                 MinIO is empty, instead of analytics_job.py
                                 silently succeeding with 0 rows everywhere.

    5. spark_analytics_job      spark-submit analytics_job.py (M3) inside
                                 `stocks-spark`. Reads clean Parquet from
                                 MinIO, writes 9-10 insight tables to
                                 Postgres via JDBC.

    6. load_post_analytics_sql  Runs post_analytics.sql (psql, not plain SQL
                                 — it uses \\echo meta-commands) inside the
                                 `stocks-postgres` container to restore the
                                 indexes/views that Spark's JDBC
                                 mode="overwrite" drops on every run.

    7. validate_data            Row-count sanity check on every table
                                 analytics_job.py is supposed to have
                                 written. Fails the DAG run if any table is
                                 empty.

DESIGN NOTE — why "docker exec" instead of DockerOperator / SparkSubmitOperator
--------------------------------------------------------------------------------
`stocks-spark`, `stocks-minio`, and `stocks-postgres` are already long-running
services in docker-compose.yml with all the right JARs, env vars, and volumes
baked in. Rather than have Airflow spin up *new* containers (which, over the
Docker socket from inside another container, needs host-absolute volume paths
and duplicates config), each task execs into the existing service container —
exactly what a human would type at the terminal, just automated. Airflow's
job here is orchestration/scheduling/retries, not re-implementing Spark.

This does mean airflow-webserver/airflow-scheduler need /var/run/docker.sock
mounted (see docker-compose.yml) and the `docker` python package (see
airflow/requirements-airflow.txt).

============================================================================
RUNNING THIS ON A NEW MACHINE (READ THIS IF THE DAG WORKS FOR ONE PERSON
BUT NOT ANOTHER) — this DAG never starts its own infrastructure. Every task
below assumes the following is already true on the machine Airflow is
running on, BEFORE you unpause the DAG:

    1. The full docker-compose stack is up:
           docker compose up -d
       and `docker compose ps` shows kafka-1, stocks-spark, stocks-minio,
       and stocks-postgres as "running" (not "restarting" / "exited").

    2. /var/run/docker.sock is mounted into the airflow-webserver AND
       airflow-scheduler containers (check docker-compose.yml — if a
       teammate is running a different compose override file, or added
       Airflow to their own docker-compose without copying that mount,
       every task will fail immediately with a docker.from_env() error).

    3. The `docker` python package is installed in the Airflow image
       (airflow/requirements-airflow.txt) — rebuild the Airflow image
       after pulling changes: `docker compose build airflow-webserver
       airflow-scheduler`.

    4. AIRFLOW_CONN_STOCKS_POSTGRES is set in the Airflow container's
       environment (.env file, not just your shell) so the
       `stocks_postgres` connection auto-registers. Without it,
       check_postgres_health / validate_data fail on every machine that
       doesn't have this connection manually added in the Airflow UI.

    5. Something is actually producing events onto the `us-stocks-raw`
       Kafka topic on that machine. If nobody's producer is running
       locally, run_spark_streaming_job will "succeed" (a timeout exit is
       expected/OK) but drain nothing, and check_minio_data will then
       correctly fail the run with "0 objects" — this is the single most
       common reason the pipeline runs cleanly for one person and not
       another on a from-scratch local stack.

    check_setup_preflight (task 0 below) checks items 1-2-3-4 automatically
    and fails with a specific, actionable message instead of a confusing
    downstream error, so a teammate can tell in one glance which of the
    above they're missing.
============================================================================
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
import docker
from docker.errors import NotFound, DockerException

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

SPARK_CONTAINER = "stocks-spark"
MINIO_CONTAINER = "stocks-minio"
POSTGRES_CONTAINER = "stocks-postgres"
KAFKA_CONTAINER = "kafka-1"
KAFKA_BOOTSTRAP = "kafka-1:29092"
POSTGRES_CONN_ID = "stocks_postgres"  # auto-registered via AIRFLOW_CONN_STOCKS_POSTGRES

# Every container each task exec's into. Used by the preflight check so a
# teammate gets ONE clear error listing every missing container, instead of
# discovering them one at a time as each task fails in turn.
ALL_REQUIRED_CONTAINERS = [KAFKA_CONTAINER, SPARK_CONTAINER, MINIO_CONTAINER, POSTGRES_CONTAINER]

# Topics kafka-init creates on stack startup — see docker-compose.yml
EXPECTED_TOPICS = ["us-stocks-raw", "us-stocks-dead-letter"]

# How long to let the Spark Structured Streaming job (M2) drain Kafka into
# MinIO before this batch DAG run moves on to analytics. Structured Streaming
# jobs run forever by design, so a scheduled batch DAG has to bound this
# somehow. Kept comfortably under the schedule cadence (see `schedule` below)
# so there's still time left in each cycle for the MinIO check, SparkSQL
# analytics, indexing, and validation steps before the next run is due.
STREAMING_RUN_SECONDS = 180

# `timeout` exits 124 when it kills the process via SIGTERM after the given
# duration, and shells commonly report 143 (128+SIGTERM) if the underlying
# process itself surfaces the termination signal as its exit status. Both
# are the EXPECTED way this task ends — not a failure.
EXPECTED_TIMEOUT_EXIT_CODES = {124, 143}

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

# ── M2: Spark Structured Streaming (Kafka -> MinIO) ───────────────────────────
# Bounded with `timeout` — see STREAMING_RUN_SECONDS above.
SPARK_STREAMING_CMD = """
timeout {timeout}s /opt/spark/bin/spark-submit \
  --master spark://stocks-spark:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/spark/work-dir/streaming_job.py
echo "__EXIT_CODE__:$?"
""".strip()

# ── M3: SparkSQL Analytics (MinIO -> Postgres) ────────────────────────────────
SPARK_SUBMIT_CMD = """
set -e
/opt/spark/bin/spark-submit \
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

# ── MinIO check — list objects under clean/ via mc (same image as minio-init) ─
MINIO_CHECK_CMD = """
set -e
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1 \
  || mc alias set local http://minio:9000 "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" >/dev/null 2>&1
mc ls --recursive local/stocks/clean/ | wc -l
""".strip()


# ─────────────────────────────────────────────────────────────────────────────
# Shared helper — run a command inside an already-running container
# ─────────────────────────────────────────────────────────────────────────────
def _get_docker_client() -> docker.DockerClient:
    """Wraps docker.from_env() with a message that tells a teammate exactly
    what's missing on THIS machine (socket not mounted, daemon not running,
    permission denied, etc.) instead of a raw docker-py traceback."""
    try:
        client = docker.from_env()
        client.ping()
        return client
    except DockerException as exc:
        raise AirflowException(
            "Could not talk to the Docker daemon from inside the Airflow "
            "container. This almost always means /var/run/docker.sock is "
            "not mounted into airflow-webserver/airflow-scheduler on this "
            "machine — check docker-compose.yml against the version other "
            f"teammates are using. Original error: {exc}"
        ) from exc


def _exec_in_container(
    container_name: str,
    shell_command: str,
    allowed_exit_codes: set[int] | None = None,
) -> str:
    """Run `shell_command` via `bash -c` inside `container_name`, return its
    combined stdout/stderr as text, and raise AirflowException (→ Airflow
    retry) if it exits with a code not in `allowed_exit_codes` (default:
    only 0 is allowed)."""
    allowed = allowed_exit_codes or {0}

    client = _get_docker_client()
    try:
        container = client.containers.get(container_name)
    except NotFound as exc:
        raise AirflowException(
            f"Container '{container_name}' is not running on this machine. "
            f"Run `docker compose up -d` and confirm it with "
            f"`docker compose ps` before unpausing this DAG."
        ) from exc

    log.info("Executing in %s:\n%s", container_name, shell_command)
    exit_code, output = container.exec_run(["bash", "-c", shell_command], demux=False)
    text = output.decode(errors="replace")
    log.info(text)

    if exit_code not in allowed:
        raise AirflowException(
            f"Command in container '{container_name}' failed with exit code "
            f"{exit_code} (allowed: {sorted(allowed)})"
        )
    return text


def check_setup_preflight(**_context) -> None:
    """Task 0: one-shot environment check so a teammate on a fresh machine
    gets a single, specific list of what's missing instead of chasing a
    different cryptic failure in each downstream task."""
    problems: list[str] = []

    # 1-2-3: docker reachable + every required container running
    try:
        client = _get_docker_client()
        for name in ALL_REQUIRED_CONTAINERS:
            try:
                client.containers.get(name)
            except NotFound:
                problems.append(f"container '{name}' is not running (docker compose up -d)")
    except AirflowException as exc:
        problems.append(str(exc))

    # 4: Postgres connection env var present (not whether it's valid yet —
    #    check_postgres_health does that next)
    if not os.environ.get("AIRFLOW_CONN_STOCKS_POSTGRES"):
        problems.append(
            "AIRFLOW_CONN_STOCKS_POSTGRES is not set in the Airflow "
            "container's environment (.env) — the 'stocks_postgres' "
            "connection won't auto-register on this machine."
        )

    if problems:
        raise AirflowException(
            "Preflight check failed on this machine:\n- "
            + "\n- ".join(problems)
            + "\n\nSee the setup checklist in this DAG's module docstring."
        )
    log.info("Preflight OK: all required containers running, Postgres conn env var set.")


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
    minutes running Spark against it."""
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        result = hook.get_first("SELECT 1;")
    except Exception as exc:
        raise AirflowException(
            f"Could not use Airflow connection '{POSTGRES_CONN_ID}'. On a "
            f"new machine this usually means AIRFLOW_CONN_STOCKS_POSTGRES "
            f"isn't set in .env, or the connection was never added in the "
            f"Airflow UI (Admin -> Connections). Original error: {exc}"
        ) from exc
    if not result or result[0] != 1:
        raise AirflowException("Postgres health check returned an unexpected result.")
    log.info("Postgres healthy (SELECT 1 succeeded).")


def run_spark_streaming_job(**_context) -> None:
    """M2: Kafka -> MinIO. Bounded by `timeout` since Structured Streaming
    jobs otherwise run forever. A timeout-induced exit is success, not
    failure — it means the job drained Kafka into MinIO for the configured
    window and was then stopped on schedule."""
    cmd = SPARK_STREAMING_CMD.format(timeout=STREAMING_RUN_SECONDS)
    # 0 = job exited on its own before the timeout (e.g. Kafka topic was
    #     fully drained and idle); 124/143 = expected timeout termination.
    _exec_in_container(
        SPARK_CONTAINER,
        cmd,
        allowed_exit_codes={0, *EXPECTED_TIMEOUT_EXIT_CODES},
    )


def check_minio_data(**_context) -> None:
    """Confirms streaming_job.py actually wrote clean Parquet to MinIO
    before analytics_job.py reads from it. Prevents a silent 0-row
    analytics run from looking like a successful DAG."""
    output = _exec_in_container(MINIO_CONTAINER, MINIO_CHECK_CMD)
    try:
        object_count = int(output.strip().splitlines()[-1])
    except (ValueError, IndexError):
        object_count = 0

    log.info("MinIO s3a://stocks/clean/ object count: %d", object_count)
    if object_count == 0:
        raise AirflowException(
            "MinIO has no objects under s3a://stocks/clean/ after the "
            "streaming job ran. On a fresh/local machine this usually "
            "means nothing is producing onto the 'us-stocks-raw' Kafka "
            "topic yet — start a producer, or point this stack at shared "
            "Kafka infra. Also check `docker compose logs spark-worker`."
        )


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
    "retry_delay": timedelta(minutes=1),
    # SLA: if the whole run isn't done in 5.5 min, Airflow marks it as an SLA
    # miss (visible in the UI / sendable to Slack/email) without failing the
    # run outright. Kept under the 7-minute schedule interval so a slow run
    # is flagged before the next one is even due — tune this once you know
    # real Spark job durations on your hardware.
    "sla": timedelta(minutes=5, seconds=30),
}

with DAG(
    dag_id="stock_analytics_pipeline",
    description=(
        "Preflight env check -> Kafka health -> Postgres health -> Spark "
        "Streaming (Kafka->MinIO) -> MinIO data check -> SparkSQL analytics "
        "(MinIO->Postgres) -> Postgres load/index -> validation. "
        "Runs every 7 minutes."
    ),
    default_args=default_args,
<<<<<<< HEAD
    # Fires exactly every 7 minutes from the last run's start, regardless of
    # clock time. (Cron's */7 does NOT divide evenly into 60 minutes, so it
    # would produce an irregular gap once per hour — a timedelta avoids that.)
    schedule=timedelta(minutes=7),
=======
    # Every 5 minutes, anchored to the Africa/Cairo timezone (handles EET/EEST
    # DST switches automatically via the IANA tz database — no manual offset
    # math needed). start_date's time-of-day (15:00) is just the anchor point
    # cron intervals are calculated from; with catchup=False the first actual
    # run fires at the next 5-minute mark after the DAG is unpaused, and every
    # 5 minutes after that, day after day.
    schedule="*/5 * * * *",
>>>>>>> c0f203ad803863772c33a7edc7575a049839624a
    start_date=pendulum.datetime(2026, 1, 1, 15, 0, tz="Africa/Cairo"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=5),
    max_active_runs=1,          # never let two pipeline runs overlap — if a
                                 # run takes longer than 7 min, the next one
                                 # queues behind it instead of running in
                                 # parallel.
    tags=[
        "US Stocks Pipeline",
        "0. Preflight - Env Check",
        "1. Kafka - Ingest",
        "2. Spark Streaming - Process",
        "3. MinIO - Store",
        "4. SparkSQL - Analyze",
        "5. PostgreSQL - Serve",
    ],
) as dag:

    preflight = PythonOperator(
        task_id="check_setup_preflight",
        python_callable=check_setup_preflight,
    )

    check_kafka = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka_health,
    )

    check_postgres = PythonOperator(
        task_id="check_postgres_health",
        python_callable=check_postgres_health,
    )

    spark_streaming_job = PythonOperator(
        task_id="run_spark_streaming_job",
        python_callable=run_spark_streaming_job,
        execution_timeout=timedelta(seconds=STREAMING_RUN_SECONDS + 120),
    )

    minio_check = PythonOperator(
        task_id="check_minio_data",
        python_callable=check_minio_data,
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

    # Preflight -> Kafka/Postgres health -> Spark Streaming -> MinIO check
    # -> SparkSQL Analytics -> Postgres load -> validate
    preflight >> [check_kafka, check_postgres] >> spark_streaming_job >> minio_check \
        >> spark_analytics_job >> load_post_analytics_sql >> validate