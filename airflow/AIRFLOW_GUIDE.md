# 🌬️ Airflow Guide — Real-Time US Stocks Data Pipeline

> Orchestrates the full pipeline: **Kafka → Spark Streaming → MinIO → SparkSQL → PostgreSQL**

---

## 📌 Table of Contents

- [Accessing Airflow](#-accessing-airflow)
- [Starting / Stopping Airflow](#-starting--stopping-airflow)
- [The Pipeline DAG](#-the-pipeline-dag)
- [Using the Web UI](#-using-the-web-ui)
- [Using the CLI](#-using-the-cli)
- [Reading Task Logs](#-reading-task-logs)
- [Common Tasks](#-common-tasks)
- [Troubleshooting](#-troubleshooting)

---

## 🔑 Accessing Airflow

| Setting | Value |
|---|---|
| URL | http://localhost:8082 |
| Username | `AIRFLOW_ADMIN_USER` from your `.env` (default: `admin`) |
| Password | `AIRFLOW_ADMIN_PASSWORD` from your `.env` (default: `admin`) |

> Port `8082` is used because `8080` is taken by Kafka UI and `8081` by the Spark Master UI.

---

## ▶️ Starting / Stopping Airflow

### Start the whole stack (Kafka + Spark + MinIO + Postgres + Airflow)

```powershell
docker compose up -d
```

### Start only the Airflow services (rest of the stack already running)

```powershell
docker compose up -d postgres-airflow airflow-init airflow-webserver airflow-scheduler
```

### Check Airflow's containers are healthy

```powershell
docker compose ps airflow-webserver airflow-scheduler
```

### Restart Airflow after editing the DAG file

The scheduler polls `dags/` for changes roughly every 30 seconds, so most edits pick up on their own. To force it immediately:

```powershell
docker compose restart airflow-scheduler
```

### Stop everything

```powershell
docker compose down            # keeps data (Kafka, MinIO, Postgres, Airflow history)
docker compose down -v         # full reset — deletes ALL data including Airflow run history
```

---

## 🗺 The Pipeline DAG

**DAG ID:** `stock_analytics_pipeline`

**Schedule:** every 5 minutes, anchored to `Africa/Cairo` time (handles EET/EEST automatically)

**Task order:**

```
                    ┌── check_kafka_health ────┐
                    │                          │
                    └── check_postgres_health ─┴──► run_spark_streaming_job
                                                          (Kafka → MinIO)
                                                                │
                                                                ▼
                                                        check_minio_data
                                                                │
                                                                ▼
                                                       spark_analytics_job
                                                        (MinIO → Postgres)
                                                                │
                                                                ▼
                                                    load_post_analytics_sql
                                                     (restore indexes/views)
                                                                │
                                                                ▼
                                                          validate_data
                                                     (row-count sanity check)
```

| Task | What it does | Fails if |
|---|---|---|
| `check_kafka_health` | Lists Kafka topics via `kafka-1` | Brokers down or expected topics missing |
| `check_postgres_health` | Runs `SELECT 1` against Postgres | Postgres unreachable |
| `run_spark_streaming_job` | Runs `streaming_job.py` inside `stocks-spark` for a bounded window (`STREAMING_RUN_SECONDS`, default 180s) | Spark session fails to start |
| `check_minio_data` | Lists objects under `s3a://stocks/clean/` via `mc` | Streaming job produced 0 files |
| `spark_analytics_job` | Runs `analytics_job.py` — 9-10 SparkSQL insight tables → Postgres | Spark/S3A/JDBC error |
| `load_post_analytics_sql` | Restores indexes/views Spark's `mode=overwrite` drops | `post_analytics.sql` errors |
| `validate_data` | Confirms every expected table has rows | Any table is empty/missing |

---

## 🖥 Using the Web UI

### 1. Log in
Go to http://localhost:8082 and sign in with your admin credentials.

### 2. Find the DAG
On the **DAGs** list page, look for `stock_analytics_pipeline`. It starts **paused** — you must unpause it (toggle switch on the left of the row) before it will run on schedule.

### 3. View runs — the Grid view
Click the DAG name to open the **Grid** view. Each column is one run; each row is one task. Colors mean:

| Color | Meaning |
|---|---|
| 🟩 Green | Success |
| 🟥 Red | Failed |
| 🟨 Yellow | Up for retry |
| ⬜ Grey | Not yet started / skipped |
| 🟦 Light blue | Running |

### 4. Read a task's log
Click any colored square → a side panel opens → click **Log**. This shows the exact command that ran inside the container and any error output — this is the first place to look whenever a task is red.

### 5. Trigger a run manually
Click the ▶️ (play) button top-right of the DAG page to run it immediately instead of waiting for the next scheduled slot.

### 6. Pause / unpause
The toggle switch next to the DAG name on the DAGs list page. Paused = scheduler won't create new runs, but you can still trigger manually.

---

## 💻 Using the CLI

All commands run through the `airflow-webserver` container:

```powershell
docker exec -it airflow-webserver airflow <command>
```

| Task | Command |
|---|---|
| Unpause the DAG | `airflow dags unpause stock_analytics_pipeline` |
| Pause the DAG | `airflow dags pause stock_analytics_pipeline` |
| Trigger a run now | `airflow dags trigger stock_analytics_pipeline` |
| List recent runs | `airflow dags list-runs -d stock_analytics_pipeline` |
| List only running runs | `airflow dags list-runs -d stock_analytics_pipeline --state running` |
| Mark a run as failed (stop it) | `airflow dags set-state stock_analytics_pipeline <run_id> --state failed` |
| Clear a task (re-run it) | `airflow tasks clear stock_analytics_pipeline -y` |
| Show a task's log | `airflow tasks logs stock_analytics_pipeline <task_id> <run_id>` |
| List all DAGs | `airflow dags list` |

> Full example (Windows PowerShell):
> ```powershell
> docker exec -it airflow-webserver airflow dags trigger stock_analytics_pipeline
> ```

---

## 📜 Reading Task Logs

**From the UI (easiest):** Grid view → click the task square → Log tab.

**From the CLI:**
```powershell
docker exec -it airflow-webserver airflow tasks logs stock_analytics_pipeline spark_analytics_job <run_id>
```
Get `<run_id>` from the Grid view URL or from `airflow dags list-runs -d stock_analytics_pipeline`.

**Underlying container logs** (useful when the Airflow task itself succeeded in *launching* something, but you want to see the raw Spark/Postgres output):
```powershell
docker compose logs -f spark-worker
docker compose logs -f postgres
```

---

## ✅ Common Tasks

### Run the pipeline right now, don't wait for the schedule
```powershell
docker exec -it airflow-webserver airflow dags trigger stock_analytics_pipeline
```

### Re-run a specific failed task without re-running the whole DAG
In the UI: click the failed task square → **Clear task** → confirm. The scheduler picks it back up automatically.

### Change how long the streaming step runs each cycle
Edit `STREAMING_RUN_SECONDS` near the top of `dags/stock_pipeline_dag.py`, save, then `docker compose restart airflow-scheduler`.

### Change the schedule
Edit the `schedule=` and `start_date=` arguments in the `with DAG(...)` block of `dags/stock_pipeline_dag.py`.

### See what's currently deployed inside the container
```powershell
docker exec -it airflow-webserver cat /opt/airflow/dags/stock_pipeline_dag.py
```
(useful to confirm an edit you made on the host actually reached the container — `dags/` is a bind mount, so it should always match)

---

## 🐛 Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| DAG doesn't appear in the UI | Scheduler hasn't parsed it yet, or a syntax error in the file | `docker compose logs airflow-scheduler` — look for import errors |
| Task fails: `command not found` (exit code 127) | A shell command inside `_exec_in_container` isn't on that container's `PATH` | Use an absolute path (e.g. `/opt/spark/bin/spark-submit` instead of `spark-submit`) |
| `check_kafka_health` fails | Kafka brokers not healthy yet, or `kafka-init` hasn't finished | `docker compose ps` — wait for brokers to show `healthy` |
| `check_minio_data` fails with "no objects" | Streaming job's window (`STREAMING_RUN_SECONDS`) was too short, or the producer isn't sending data | `docker compose logs producer`; consider raising `STREAMING_RUN_SECONDS` |
| `spark_analytics_job` fails on JDBC/Postgres | `stocks-postgres` unreachable from inside `stocks-spark`, or wrong credentials | Confirm both containers are on the `stocks-net` network and env vars match `.env` |
| Runs pile up / DAG looks "stuck catching up" | A single run is taking longer than the 5-minute schedule interval, and `max_active_runs=1` queues the next one behind it | Check task durations in the Grid view (hover a task square); raise the schedule interval or shorten `STREAMING_RUN_SECONDS` |
| Container not found errors from `_exec_in_container` | The target container (`stocks-spark`, `stocks-minio`, `stocks-postgres`, `kafka-1`) isn't running | `docker compose ps` and start whatever's missing |
| Edits to the DAG file don't seem to take effect | Old file still cached somewhere, or you edited the wrong path | Confirm with `docker exec -it airflow-webserver cat /opt/airflow/dags/stock_pipeline_dag.py`, then `docker compose restart airflow-scheduler` |

---

## 🔗 Related Docs

- `PIPELINE_QUICKSTART.md` — full manual run-through of every pipeline stage without Airflow
- `spark/README.md` — Spark Streaming details
- `SparkSQL/README.md` — SparkSQL Analytics details
- `PostgresSQL/README.md` — PostgreSQL setup, schema, and Power BI connection
