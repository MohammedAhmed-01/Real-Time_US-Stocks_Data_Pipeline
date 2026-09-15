# 🖥️ Docker Swarm Cluster على Windows
## Real-Time US Stocks Data Pipeline
### 1 Master + 2 Workers — خطوة بخطوة

---

## معلومات الـ Cluster ✅ مكتملة

```
MASTER_HOSTNAME  = MohammedSalah        (Leader)
WORKER1_HOSTNAME = DESKTOP-N696QFV
WORKER2_HOSTNAME = osos

MASTER_IP  = 100.70.114.122
WORKER1_IP = 100.127.42.87
WORKER2_IP = 100.78.181.9

PROJECT_PATH = C:\Users\moham\Desktop\ETA_FinalProject\Real-Time_US-Stocks_Data_Pipeline
```

---

## الحالة الحالية

* [x] المرحلة 1 — WSL2 + Ubuntu ✅
* [x] المرحلة 2 — Tailscale ✅ (الثلاثة Connected)
* [x] المرحلة 3 — Docker Desktop ✅
* [x] المرحلة 4 — Firewall ports ✅
* [x] المرحلة 5.1–5.5 — Swarm init + Workers joined ✅
* [ ] **المرحلة 5.6 — Node Labels ← أنت هنا**
* [ ] المرحلة 6 — Overlay Network
* [ ] المرحلة 7 — Shared Storage (SMB)
* [ ] المرحلة 8 — Local Registry
* [ ] المرحلة 9 — Build & Push Images
* [ ] المرحلة 10 — .env file
* [ ] المرحلة 11 — docker-stack.yml
* [ ] المرحلة 12 — Deploy
* [ ] المرحلة 13 — Verify

---

## ▶️ المرحلة 5.6 — Node Labels (على الـ Master فقط)

افتح PowerShell على **MohammedSalah** وشغّل:

```powershell
# ── Role Labels ──────────────────────────────────────────
docker node update --label-add role=master MohammedSalah
docker node update --label-add role=worker DESKTOP-N696QFV
docker node update --label-add role=worker osos

# ── Kafka Broker Placement Labels ────────────────────────
docker node update --label-add kafka.broker=1 MohammedSalah
docker node update --label-add kafka.broker=2 DESKTOP-N696QFV
docker node update --label-add kafka.broker=3 osos

# ── تحقق ─────────────────────────────────────────────────
docker node inspect MohammedSalah    --format "{{ .Spec.Labels }}"
docker node inspect DESKTOP-N696QFV  --format "{{ .Spec.Labels }}"
docker node inspect osos             --format "{{ .Spec.Labels }}"
```

**الناتج المطلوب:**
```
map[kafka.broker:1 role:master]
map[kafka.broker:2 role:worker]
map[kafka.broker:3 role:worker]
```

---

## ▶️ المرحلة 6 — Overlay Network (على الـ Master فقط)

```powershell
docker network create `
    --driver overlay `
    --attachable `
    --subnet 10.20.0.0/16 `
    stocks-net

# تحقق
docker network ls | Select-String "stocks-net"
```

**الناتج المطلوب:** سطر فيه `overlay   swarm`

---

## ▶️ المرحلة 7 — Shared Storage (SMB)

### 7.1 — أنشئ المجلدات على الـ Master (PowerShell عادي)

```powershell
New-Item -ItemType Directory -Force "C:\stocks-data\minio-data"
New-Item -ItemType Directory -Force "C:\stocks-data\postgres-data"
New-Item -ItemType Directory -Force "C:\stocks-data\kafka-1-data"
New-Item -ItemType Directory -Force "C:\stocks-data\kafka-2-data"
New-Item -ItemType Directory -Force "C:\stocks-data\kafka-3-data"
New-Item -ItemType Directory -Force "C:\stocks-data\airflow-logs"
New-Item -ItemType Directory -Force "C:\stocks-data\airflow-plugins"
New-Item -ItemType Directory -Force "C:\stocks-data\airflow-postgres"
New-Item -ItemType Directory -Force "C:\stocks-data\registry"
```

### 7.2 — شارك المجلد عبر SMB (PowerShell كـ Administrator على الـ Master)

```powershell
New-SmbShare `
    -Name "stocks-data" `
    -Path "C:\stocks-data" `
    -FullAccess "Everyone" `
    -Description "Shared storage for Docker Swarm cluster"

# تحقق
Get-SmbShare -Name "stocks-data"
```

### 7.3 — افتح Firewall للـ SMB (PowerShell كـ Administrator على الـ Master)

```powershell
New-NetFirewallRule `
    -DisplayName "SMB for Tailscale" `
    -Direction Inbound `
    -Protocol TCP `
    -LocalPort 445 `
    -RemoteAddress "100.0.0.0/8" `
    -Action Allow
```

### 7.4 — اختبر الوصول من Workers

افتح PowerShell على **DESKTOP-N696QFV** وعلى **osos** وشغّل:

```powershell
Test-NetConnection -ComputerName 100.70.114.122 -Port 445
```

**المطلوب:** `TcpTestSucceeded : True`

لو فاشل — تأكد إن Tailscale شغّال على الـ Master وإن قاعدة الـ Firewall اتضافت صح.

### 7.5 — أنشئ Docker Volumes (على الثلاثة أجهزة)

> ⚠️ **قبل ما تشغّل:** استبدل `YOUR_WINDOWS_USERNAME` و`YOUR_WINDOWS_PASSWORD` ببيانات حساب Windows على جهاز **MohammedSalah**.
>
> لو مش عارف الـ username، شغّل في PowerShell: `$env:USERNAME`
>
> لو الحساب بدون كلمة مرور، ستحتاج لإضافة كلمة مرور: Settings → Accounts → Sign-in options

```powershell
$MASTER_IP = "100.70.114.122"
$SMB_USER  = "YOUR_WINDOWS_USERNAME"
$SMB_PASS  = "YOUR_WINDOWS_PASSWORD"
$SMB_OPTS  = "username=${SMB_USER},password=${SMB_PASS},vers=3.0"

docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/minio-data"      minio-data
docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/postgres-data"    postgres-data
docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/kafka-1-data"     kafka-1-data
docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/kafka-2-data"     kafka-2-data
docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/kafka-3-data"     kafka-3-data
docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/airflow-logs"     airflow-logs
docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/airflow-plugins"  airflow-plugins
docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/airflow-postgres" airflow-postgres-data
docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/registry"        registry-data

# تحقق
docker volume ls
```

**الناتج المطلوب:** ترى الـ 9 Volumes في القائمة.

---

## ▶️ المرحلة 8 — Registry محلي (على الـ Master فقط)

### 8.1 — شغّل الـ Registry

```powershell
docker run -d `
    --name registry `
    --restart always `
    -p 5000:5000 `
    -v registry-data:/var/lib/registry `
    registry:2

# تحقق
Invoke-WebRequest -Uri "http://localhost:5000/v2/_catalog" -UseBasicParsing | Select-Object Content
```

**الناتج المطلوب:** `{"repositories":[]}`

### 8.2 — أضف Registry كـ Insecure على الثلاثة أجهزة

على كل جهاز: Docker Desktop → ⚙️ Settings → Docker Engine → عدّل الـ JSON:

```json
{
  "builder": {"gc": {"defaultKeepStorage": "20GB", "enabled": true}},
  "experimental": false,
  "insecure-registries": ["100.70.114.122:5000"]
}
```

اضغط **Apply & Restart** على كل جهاز.

---

## ▶️ المرحلة 9 — Build & Push Images (على الـ Master فقط)

```powershell
cd "C:\Users\moham\Desktop\ETA_FinalProject\Real-Time_US-Stocks_Data_Pipeline"
$REG = "100.70.114.122:5000"

# Python image
docker build -t stocks-python:latest -f Dockerfile.python .
docker tag stocks-python:latest "${REG}/stocks-python:latest"
docker push "${REG}/stocks-python:latest"

# Spark image — يأخذ 10-20 دقيقة
docker build -t stocks-spark:3.5.7 -f spark\Dockerfile .\spark
docker tag stocks-spark:3.5.7 "${REG}/stocks-spark:3.5.7"
docker push "${REG}/stocks-spark:3.5.7"

# Airflow image
docker build -t stocks-airflow:2.10.4 -f airflow\Dockerfile .\airflow
docker tag stocks-airflow:2.10.4 "${REG}/stocks-airflow:2.10.4"
docker push "${REG}/stocks-airflow:2.10.4"

# تحقق — المطلوب: {"repositories":["stocks-airflow","stocks-python","stocks-spark"]}
Invoke-WebRequest -Uri "http://${REG}/v2/_catalog" -UseBasicParsing | Select-Object Content
```

### 9.1 — اسحب الـ Images على Workers

شغّل على **DESKTOP-N696QFV** وعلى **osos**:

```powershell
$REG = "100.70.114.122:5000"

docker pull "${REG}/stocks-python:latest"
docker pull "${REG}/stocks-spark:3.5.7"
docker pull "${REG}/stocks-airflow:2.10.4"

docker tag "${REG}/stocks-python:latest"   stocks-python:latest
docker tag "${REG}/stocks-spark:3.5.7"     stocks-spark:3.5.7
docker tag "${REG}/stocks-airflow:2.10.4"  stocks-airflow:2.10.4

# تحقق
docker images | Select-String "stocks"
```

---

## ▶️ المرحلة 10 — ملف `.env`

احفظ في مجلد المشروع على الـ Master باسم `.env`:

```env
# Kaggle
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key

# Kafka
KAFKA_BOOTSTRAP=kafka-1:29092,kafka-2:29093,kafka-3:29094
KAFKA_TOPIC=us-stocks-raw
KAFKA_DLT_TOPIC=us-stocks-dead-letter
KAFKA_GROUP_ID=m1-validation-group

# MinIO
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=stocks

# PostgreSQL
POSTGRES_DB=stocks_analytics
POSTGRES_USER=stocks
POSTGRES_PASSWORD=stocks123

# pgAdmin
PGADMIN_DEFAULT_EMAIL=admin@stocks.com
PGADMIN_DEFAULT_PASSWORD=admin123

# Airflow
AIRFLOW_SECRET_KEY=change-me-generate-with-python
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
```

لتوليد `AIRFLOW_SECRET_KEY`:

```powershell
python -c "import secrets; print(secrets.token_hex(32))"
```

استبدل `change-me-generate-with-python` بالناتج.

---

## ▶️ المرحلة 11 — ملف `docker-stack.yml`

احفظ في نفس مجلد المشروع باسم `docker-stack.yml`:

```yaml
version: "3.9"

# ================================================================
# Docker Swarm Stack — Real-Time US Stocks Data Pipeline
# Master:  MohammedSalah  (100.70.114.122)
# Worker1: DESKTOP-N696QFV (100.127.42.87)
# Worker2: osos            (100.78.181.9)
# ================================================================

services:

  # ── KAFKA BROKERS ────────────────────────────────────────────

  kafka-1:
    image: confluentinc/cp-kafka:7.6.1
    networks:
      - stocks-net
    ports:
      - target: 9092
        published: 9092
        mode: host
    environment:
      KAFKA_NODE_ID:   1
      CLUSTER_ID:      MkU3OEVBNTcwNTJENDM2Qk
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: CONTROLLER://0.0.0.0:29091,PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-1:29092,PLAINTEXT_HOST://100.70.114.122:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME:     PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES:      CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS:       1@kafka-1:29091,2@kafka-2:29091,3@kafka-3:29091
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:         "3"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "3"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR:            "2"
      KAFKA_DEFAULT_REPLICATION_FACTOR:               "3"
      KAFKA_MIN_INSYNC_REPLICAS:                      "2"
      KAFKA_NUM_PARTITIONS:                           "6"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE:                "false"
      KAFKA_DELETE_TOPIC_ENABLE:                      "true"
      KAFKA_LOG_RETENTION_HOURS:                      "168"
      KAFKA_COMPRESSION_TYPE:                         lz4
      KAFKA_LOG_DIRS:                                 /var/lib/kafka/data
    volumes:
      - kafka-1-data:/var/lib/kafka/data
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.kafka.broker == 1
      restart_policy:
        condition: on-failure
        delay: 15s
        max_attempts: 5

  kafka-2:
    image: confluentinc/cp-kafka:7.6.1
    networks:
      - stocks-net
    ports:
      - target: 9093
        published: 9093
        mode: host
    environment:
      KAFKA_NODE_ID:   2
      CLUSTER_ID:      MkU3OEVBNTcwNTJENDM2Qk
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: CONTROLLER://0.0.0.0:29091,PLAINTEXT://0.0.0.0:29093,PLAINTEXT_HOST://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-2:29093,PLAINTEXT_HOST://100.127.42.87:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME:     PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES:      CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS:       1@kafka-1:29091,2@kafka-2:29091,3@kafka-3:29091
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:         "3"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "3"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR:            "2"
      KAFKA_DEFAULT_REPLICATION_FACTOR:               "3"
      KAFKA_MIN_INSYNC_REPLICAS:                      "2"
      KAFKA_NUM_PARTITIONS:                           "6"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE:                "false"
      KAFKA_DELETE_TOPIC_ENABLE:                      "true"
      KAFKA_LOG_RETENTION_HOURS:                      "168"
      KAFKA_COMPRESSION_TYPE:                         lz4
      KAFKA_LOG_DIRS:                                 /var/lib/kafka/data
    volumes:
      - kafka-2-data:/var/lib/kafka/data
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.kafka.broker == 2
      restart_policy:
        condition: on-failure
        delay: 15s
        max_attempts: 5

  kafka-3:
    image: confluentinc/cp-kafka:7.6.1
    networks:
      - stocks-net
    ports:
      - target: 9094
        published: 9094
        mode: host
    environment:
      KAFKA_NODE_ID:   3
      CLUSTER_ID:      MkU3OEVBNTcwNTJENDM2Qk
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: CONTROLLER://0.0.0.0:29091,PLAINTEXT://0.0.0.0:29094,PLAINTEXT_HOST://0.0.0.0:9094
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-3:29094,PLAINTEXT_HOST://100.78.181.9:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME:     PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES:      CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS:       1@kafka-1:29091,2@kafka-2:29091,3@kafka-3:29091
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:         "3"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "3"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR:            "2"
      KAFKA_DEFAULT_REPLICATION_FACTOR:               "3"
      KAFKA_MIN_INSYNC_REPLICAS:                      "2"
      KAFKA_NUM_PARTITIONS:                           "6"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE:                "false"
      KAFKA_DELETE_TOPIC_ENABLE:                      "true"
      KAFKA_LOG_RETENTION_HOURS:                      "168"
      KAFKA_COMPRESSION_TYPE:                         lz4
      KAFKA_LOG_DIRS:                                 /var/lib/kafka/data
    volumes:
      - kafka-3-data:/var/lib/kafka/data
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.kafka.broker == 3
      restart_policy:
        condition: on-failure
        delay: 15s
        max_attempts: 5

  # ── KAFKA INIT ───────────────────────────────────────────────

  kafka-init:
    image: confluentinc/cp-kafka:7.6.1
    networks:
      - stocks-net
    entrypoint: ["/bin/bash", "-c"]
    command:
      - |
        set -e
        echo 'Waiting 60s for all 3 brokers...'
        sleep 60
        kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists \
          --topic us-stocks-raw --partitions 6 --replication-factor 3 \
          --config retention.ms=604800000 \
          --config compression.type=lz4 \
          --config min.insync.replicas=2
        kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists \
          --topic us-stocks-dead-letter --partitions 3 --replication-factor 3 \
          --config retention.ms=2592000000 \
          --config compression.type=lz4
        kafka-topics --bootstrap-server kafka-1:29092 --list
        echo 'Topics ready.'
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.role == master
      restart_policy:
        condition: none

  # ── KAFKA UI ─────────────────────────────────────────────────

  kafka-ui:
    image: ghcr.io/kafbat/kafka-ui:latest
    networks:
      - stocks-net
    ports:
      - target: 8080
        published: 8080
        mode: host
    environment:
      KAFKA_CLUSTERS_0_NAME:             us-stocks-cluster
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka-1:29092,kafka-2:29093,kafka-3:29094
      DYNAMIC_CONFIG_ENABLED:            "true"
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.role == master
      restart_policy:
        condition: on-failure
        delay: 10s

  # ── MINIO ────────────────────────────────────────────────────

  minio:
    image: elestio/minio:latest
    networks:
      - stocks-net
    ports:
      - target: 9000
        published: 9000
        mode: host
      - target: 9001
        published: 9001
        mode: host
    environment:
      MINIO_ROOT_USER:     ${MINIO_ACCESS_KEY:-minioadmin}
      MINIO_ROOT_PASSWORD: ${MINIO_SECRET_KEY:-minioadmin}
    command: server --address ":9000" --console-address ":9001" /data
    volumes:
      - minio-data:/data
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.role == master
      restart_policy:
        condition: on-failure
        delay: 10s

  minio-init:
    image: elestio/minio:latest
    networks:
      - stocks-net
    environment:
      MINIO_ACCESS_KEY: ${MINIO_ACCESS_KEY:-minioadmin}
      MINIO_SECRET_KEY: ${MINIO_SECRET_KEY:-minioadmin}
    entrypoint: ["/bin/sh", "-c"]
    command:
      - |
        set -e
        sleep 15
        mc alias set local http://minio:9000 "$${MINIO_ACCESS_KEY}" "$${MINIO_SECRET_KEY}"
        mc mb --ignore-existing local/stocks
        echo 'Bucket stocks is ready.'
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.role == master
      restart_policy:
        condition: none

  # ── POSTGRESQL ───────────────────────────────────────────────

  postgres:
    image: postgres:16
    networks:
      - stocks-net
    ports:
      - target: 5432
        published: 5432
        mode: host
    environment:
      POSTGRES_DB:               ${POSTGRES_DB:-stocks_analytics}
      POSTGRES_USER:             ${POSTGRES_USER:-stocks}
      POSTGRES_PASSWORD:         ${POSTGRES_PASSWORD:-stocks123}
      POSTGRES_HOST_AUTH_METHOD: md5
      POSTGRES_INITDB_ARGS:      "--auth-host=md5"
    volumes:
      - postgres-data:/var/lib/postgresql/data
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.role == master
      restart_policy:
        condition: on-failure
        delay: 15s
        max_attempts: 10

  # ── SPARK MASTER ─────────────────────────────────────────────

  spark:
    image: stocks-spark:3.5.7
    networks:
      - stocks-net
    command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
    ports:
      - target: 7077
        published: 7077
        mode: host
      - target: 8080
        published: 8081
        mode: host
    environment:
      MINIO_ENDPOINT:    http://minio:9000
      MINIO_ACCESS_KEY:  ${MINIO_ACCESS_KEY:-minioadmin}
      MINIO_SECRET_KEY:  ${MINIO_SECRET_KEY:-minioadmin}
      MINIO_BUCKET:      ${MINIO_BUCKET:-stocks}
      POSTGRES_HOST:     postgres
      POSTGRES_PORT:     5432
      POSTGRES_DB:       ${POSTGRES_DB:-stocks_analytics}
      POSTGRES_USER:     ${POSTGRES_USER:-stocks}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-stocks123}
    volumes:
      - type: bind
        source: C:\Users\moham\Desktop\ETA_FinalProject\Real-Time_US-Stocks_Data_Pipeline\spark
        target: /opt/spark/work-dir
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.role == master
      restart_policy:
        condition: on-failure
        delay: 10s

  # ── SPARK WORKERS ────────────────────────────────────────────

  spark-worker-1:
    image: stocks-spark:3.5.7
    networks:
      - stocks-net
    command:
      - /opt/spark/bin/spark-class
      - org.apache.spark.deploy.worker.Worker
      - spark://spark:7077
    environment:
      SPARK_WORKER_MEMORY: 4g
      SPARK_WORKER_CORES:  4
      MINIO_ENDPOINT:    http://minio:9000
      MINIO_ACCESS_KEY:  ${MINIO_ACCESS_KEY:-minioadmin}
      MINIO_SECRET_KEY:  ${MINIO_SECRET_KEY:-minioadmin}
      MINIO_BUCKET:      ${MINIO_BUCKET:-stocks}
      POSTGRES_HOST:     postgres
      POSTGRES_PORT:     5432
      POSTGRES_DB:       ${POSTGRES_DB:-stocks_analytics}
      POSTGRES_USER:     ${POSTGRES_USER:-stocks}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-stocks123}
    volumes:
      - type: bind
        source: C:\Users\moham\Desktop\ETA_FinalProject\Real-Time_US-Stocks_Data_Pipeline\spark
        target: /opt/spark/work-dir
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.kafka.broker == 2
      restart_policy:
        condition: on-failure
        delay: 10s

  spark-worker-2:
    image: stocks-spark:3.5.7
    networks:
      - stocks-net
    command:
      - /opt/spark/bin/spark-class
      - org.apache.spark.deploy.worker.Worker
      - spark://spark:7077
    environment:
      SPARK_WORKER_MEMORY: 4g
      SPARK_WORKER_CORES:  4
      MINIO_ENDPOINT:    http://minio:9000
      MINIO_ACCESS_KEY:  ${MINIO_ACCESS_KEY:-minioadmin}
      MINIO_SECRET_KEY:  ${MINIO_SECRET_KEY:-minioadmin}
      MINIO_BUCKET:      ${MINIO_BUCKET:-stocks}
      POSTGRES_HOST:     postgres
      POSTGRES_PORT:     5432
      POSTGRES_DB:       ${POSTGRES_DB:-stocks_analytics}
      POSTGRES_USER:     ${POSTGRES_USER:-stocks}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-stocks123}
    volumes:
      - type: bind
        source: C:\Users\moham\Desktop\ETA_FinalProject\Real-Time_US-Stocks_Data_Pipeline\spark
        target: /opt/spark/work-dir
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.kafka.broker == 3
      restart_policy:
        condition: on-failure
        delay: 10s

  # ── PRODUCER & CONSUMER ──────────────────────────────────────

  producer:
    image: stocks-python:latest
    networks:
      - stocks-net
    environment:
      KAFKA_BOOTSTRAP: kafka-1:29092,kafka-2:29093,kafka-3:29094
      KAFKA_TOPIC:     us-stocks-raw
      KAGGLE_USERNAME: ${KAGGLE_USERNAME}
      KAGGLE_KEY:      ${KAGGLE_KEY}
    command:
      - python
      - producer.py
      - --rows-per-sec
      - "10000"
      - --batch-size
      - "2000"
      - --stock-list
      - /app/Stock_List.csv
    volumes:
      - type: bind
        source: C:\Users\moham\Desktop\ETA_FinalProject\Real-Time_US-Stocks_Data_Pipeline\kafka
        target: /app
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.role == master
      restart_policy:
        condition: on-failure
        delay: 10s

  consumer:
    image: stocks-python:latest
    networks:
      - stocks-net
    environment:
      KAFKA_BOOTSTRAP: kafka-1:29092,kafka-2:29093,kafka-3:29094
      KAFKA_TOPIC:     us-stocks-raw
      KAFKA_GROUP_ID:  m1-validation-group
    command:
      - python
      - consumer.py
      - --batch-size
      - "100"
      - --max-wait-ms
      - "5000"
      - --idle-timeout
      - "180"
    volumes:
      - type: bind
        source: C:\Users\moham\Desktop\ETA_FinalProject\Real-Time_US-Stocks_Data_Pipeline\kafka
        target: /app
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.role == master
      restart_policy:
        condition: on-failure
        delay: 10s

  # ── PGADMIN ──────────────────────────────────────────────────

  pgadmin:
    image: dpage/pgadmin4:latest
    networks:
      - stocks-net
    ports:
      - target: 80
        published: 5050
        mode: host
    environment:
      PGADMIN_DEFAULT_EMAIL:    ${PGADMIN_DEFAULT_EMAIL:-admin@stocks.com}
      PGADMIN_DEFAULT_PASSWORD: ${PGADMIN_DEFAULT_PASSWORD:-admin123}
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.role == master
      restart_policy:
        condition: on-failure

  # ── AIRFLOW ──────────────────────────────────────────────────

  postgres-airflow:
    image: postgres:16
    networks:
      - stocks-net
    environment:
      POSTGRES_DB:       airflow
      POSTGRES_USER:     airflow
      POSTGRES_PASSWORD: airflow
    volumes:
      - airflow-postgres-data:/var/lib/postgresql/data
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.role == master
      restart_policy:
        condition: on-failure

  airflow-init:
    image: stocks-airflow:2.10.4
    networks:
      - stocks-net
    environment: &airflow-env
      AIRFLOW__CORE__EXECUTOR:             LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres-airflow/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES:        "false"
      AIRFLOW__CORE__DEFAULT_TIMEZONE:     utc
      AIRFLOW__WEBSERVER__SECRET_KEY:      ${AIRFLOW_SECRET_KEY:-please-change-me}
      AIRFLOW_CONN_STOCKS_POSTGRES:        postgresql://${POSTGRES_USER:-stocks}:${POSTGRES_PASSWORD:-stocks123}@postgres:5432/${POSTGRES_DB:-stocks_analytics}
    entrypoint: ["/bin/bash", "-c"]
    command:
      - |
        airflow db migrate
        airflow users create \
          --username "${AIRFLOW_ADMIN_USER:-admin}" \
          --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
          --firstname Admin --lastname User \
          --role Admin --email admin@stocks.com || true
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.role == master
      restart_policy:
        condition: none

  airflow-webserver:
    image: stocks-airflow:2.10.4
    networks:
      - stocks-net
    ports:
      - target: 8080
        published: 8082
        mode: host
    environment: *airflow-env
    volumes:
      - type: bind
        source: C:\Users\moham\Desktop\ETA_FinalProject\Real-Time_US-Stocks_Data_Pipeline\dags
        target: /opt/airflow/dags
      - airflow-logs:/opt/airflow/logs
      - airflow-plugins:/opt/airflow/plugins
    command: ["airflow", "webserver"]
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.role == master
      restart_policy:
        condition: on-failure
        delay: 15s

  airflow-scheduler:
    image: stocks-airflow:2.10.4
    networks:
      - stocks-net
    environment: *airflow-env
    volumes:
      - type: bind
        source: C:\Users\moham\Desktop\ETA_FinalProject\Real-Time_US-Stocks_Data_Pipeline\dags
        target: /opt/airflow/dags
      - airflow-logs:/opt/airflow/logs
      - airflow-plugins:/opt/airflow/plugins
    command: ["airflow", "scheduler"]
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.role == master
      restart_policy:
        condition: on-failure
        delay: 15s

# ================================================================
# NETWORKS & VOLUMES
# ================================================================

networks:
  stocks-net:
    external: true

volumes:
  kafka-1-data:
    external: true
  kafka-2-data:
    external: true
  kafka-3-data:
    external: true
  minio-data:
    external: true
  postgres-data:
    external: true
  airflow-postgres-data:
    external: true
  airflow-logs:
    external: true
  airflow-plugins:
    external: true
```

---

## ▶️ المرحلة 12 — Deploy

```powershell
cd "C:\Users\moham\Desktop\ETA_FinalProject\Real-Time_US-Stocks_Data_Pipeline"

# ── تحقق من كل حاجة قبل الـ Deploy ──────────────────────
docker node ls
docker network ls | Select-String stocks-net
docker volume ls | Select-String -Pattern "kafka|minio|postgres|airflow"
docker images | Select-String "stocks"
Get-Content .env | Select-String -NotMatch "^#" | Where-Object { $_ -ne "" }

# ── Deploy ────────────────────────────────────────────────
docker stack deploy `
    --compose-file docker-stack.yml `
    --with-registry-auth `
    stocks

# ── تابع بشكل مستمر ──────────────────────────────────────
while ($true) { Clear-Host; docker stack services stocks; Start-Sleep 5 }
```

---

## ▶️ المرحلة 13 — التحقق النهائي

```powershell
# ── Kafka Topics ─────────────────────────────────────────
$KAFKA1 = docker ps --filter "name=stocks_kafka-1" --format "{{.ID}}"
docker exec $KAFKA1 kafka-topics `
    --bootstrap-server kafka-1:29092,kafka-2:29093,kafka-3:29094 `
    --list

# المطلوب: us-stocks-dead-letter و us-stocks-raw

# ── Spark Streaming ──────────────────────────────────────
$SPARK = docker ps --filter "name=stocks_spark" --format "{{.ID}}"
docker exec -it $SPARK `
    /opt/spark/bin/spark-submit `
    --master spark://spark:7077 `
    --conf spark.jars.ivy=/tmp/.ivy2 `
    /opt/spark/work-dir/streaming_job.py

# ── Analytics Job ────────────────────────────────────────
docker exec -it $SPARK `
    /opt/spark/bin/spark-submit `
    --master local[4] `
    --conf spark.jars.ivy=/tmp/.ivy2 `
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
    --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" `
    --conf spark.hadoop.fs.s3a.path.style.access=true `
    --conf spark.hadoop.fs.s3a.access.key=minioadmin `
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin `
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false `
    /opt/spark/work-dir/analytics_job.py

# ── post_analytics.sql ───────────────────────────────────
$PG = docker ps --filter "name=stocks_postgres" --format "{{.ID}}"
docker cp spark\post_analytics.sql ${PG}:/tmp/post_analytics.sql
docker exec $PG psql -U stocks -d stocks_analytics -f /tmp/post_analytics.sql
```

---

## 🌐 URLs بعد الـ Deploy

| Service | URL | Login |
|---|---|---|
| Kafka UI | http://100.70.114.122:8080 | — |
| Spark Master UI | http://100.70.114.122:8081 | — |
| MinIO Console | http://100.70.114.122:9001 | minioadmin / minioadmin |
| pgAdmin | http://100.70.114.122:5050 | admin@stocks.com / admin123 |
| Airflow UI | http://100.70.114.122:8082 | admin / admin |
| PostgreSQL | 100.70.114.122:5432 | stocks / stocks123 |

---

## 📊 توزيع الـ Services

| Service | MohammedSalah | DESKTOP-N696QFV | osos |
|---|---|---|---|
| kafka-1 | ✅ | | |
| kafka-2 | | ✅ | |
| kafka-3 | | | ✅ |
| kafka-init | ✅ | | |
| kafka-ui | ✅ | | |
| minio + minio-init | ✅ | | |
| postgres | ✅ | | |
| spark (master) | ✅ | | |
| spark-worker-1 | | ✅ | |
| spark-worker-2 | | | ✅ |
| producer | ✅ | | |
| consumer | ✅ | | |
| pgadmin | ✅ | | |
| postgres-airflow | ✅ | | |
| airflow-init | ✅ | | |
| airflow-webserver | ✅ | | |
| airflow-scheduler | ✅ | | |

---

## 🔧 أوامر مراقبة مفيدة

```powershell
# حالة الـ Cluster
docker node ls
docker stack services stocks
docker stack ps stocks

# Logs
docker service logs stocks_kafka-1 --tail 50 -f
docker service logs stocks_producer -f

# أوقف / أعد تشغيل service
docker service scale stocks_consumer=0
docker service scale stocks_consumer=1

# Force redeploy
docker service update --force stocks_producer

# إيقاف كل شيء (البيانات تبقى)
docker stack rm stocks
```

---

## 🐛 Troubleshooting

```powershell
# Service عالق في Pending
docker service ps stocks_SERVICENAME --no-trunc

# Kafka DNS check
docker run --rm --network stocks-net alpine nslookup kafka-1
docker run --rm --network stocks-net alpine nslookup kafka-2
docker run --rm --network stocks-net alpine nslookup kafka-3

# SMB Volume فاشل
Test-NetConnection -ComputerName 100.70.114.122 -Port 445
net use Z: \\100.70.114.122\stocks-data /user:MASTER_USERNAME MASTER_PASSWORD
cmdkey /add:100.70.114.122 /user:MASTER_USERNAME /pass:MASTER_PASSWORD

# Worker مش بيتجوّين
Test-NetConnection -ComputerName 100.70.114.122 -Port 2377

# Tailscale فقد الاتصال
& "C:\Program Files\Tailscale\tailscale.exe" status
& "C:\Program Files\Tailscale\tailscale.exe" down
& "C:\Program Files\Tailscale\tailscale.exe" up
```

---

*September 2026 | Real-Time US Stocks Data Pipeline*
*MohammedSalah (Master · 100.70.114.122) · DESKTOP-N696QFV (Worker1 · 100.127.42.87) · osos (Worker2 · 100.78.181.9)*
