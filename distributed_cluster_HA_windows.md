# 🖥️ Docker Swarm Cluster على Windows
## Real-Time US Stocks Data Pipeline
### 1 Master + 2 Workers — خطوة بخطوة

---

## 📌 قبل ما تبدأ — سجّل المعلومات دي

افتح PowerShell على كل جهاز وشغّل `hostname` — سجّل الناتج هنا:

```
MASTER_HOSTNAME = _______________   ← الجهاز الأقوى
WORKER1_HOSTNAME = ______________
WORKER2_HOSTNAME = ______________
```

بعد ما تثبّت Tailscale (خطوة 2)، سجّل الـ IPs هنا:

```
MASTER_IP  = 100.___.___.___ 
WORKER1_IP = 100.___.___.___ 
WORKER2_IP = 100.___.___.___ 
```

---

## المرحلة 1 — تثبيت WSL2 وUbuntu

> ⚠️ **على الثلاثة أجهزة بنفس الترتيب**

### الخطوة 1.1 — افتح PowerShell كـ Administrator

ابحث عن PowerShell في Start Menu → كليك يمين → **Run as Administrator**

### الخطوة 1.2 — فعّل WSL2

```powershell
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart
```

**أعد تشغيل الجهاز الآن.**

### الخطوة 1.3 — بعد الـ Restart، ثبّت Ubuntu

افتح PowerShell كـ Administrator مجدداً:

```powershell
wsl --set-default-version 2
wsl --install -d Ubuntu-22.04
```

سيطلب منك إدخال **username** وـ **password** لـ Ubuntu. أدخل أي اسم تحبه (مثلاً `stocks`) وكلمة مرور.

### الخطوة 1.4 — تحقق

```powershell
wsl --list --verbose
```

المطلوب: ترى `Ubuntu-22.04` بجانبها `Running` وإصدار `2`.

---

## المرحلة 2 — تثبيت Tailscale

> ⚠️ **على الثلاثة أجهزة — استخدم نفس الـ Account**

### الخطوة 2.1 — حمّل وثبّت

1. افتح المتصفح: **https://tailscale.com/download/windows**
2. حمّل وشغّل الـ installer
3. بعد التثبيت ستجد Tailscale icon في الـ System Tray (بجانب الساعة)

### الخطوة 2.2 — سجّل الدخول

1. كليك يمين على الـ Tailscale icon في الـ System Tray
2. اختر **Log in**
3. في المتصفح: سجّل دخول بـ Google أو GitHub أو Microsoft
4. **مهم جداً:** استخدم **نفس الـ Account** على الثلاثة أجهزة

### الخطوة 2.3 — اعرف الـ Tailscale IP لكل جهاز

شغّل في PowerShell على كل جهاز:

```powershell
& "C:\Program Files\Tailscale\tailscale.exe" ip -4
```

أو:

```powershell
Get-NetIPAddress -AddressFamily IPv4 | Where-Object {$_.IPAddress -like "100.*"} | Select-Object IPAddress
```

**سجّل الـ IPs في الجدول أعلاه.**

### الخطوة 2.4 — اختبر الاتصال بين الأجهزة

على الـ Master، شغّل:

```powershell
# استبدل بالـ IPs الحقيقية
ping 100.x.x.2 -n 4
ping 100.x.x.3 -n 4
```

**لازم كل الـ pings تنجح قبل ما تكمل.**

---

## المرحلة 3 — تثبيت Docker Desktop

> ⚠️ **على الثلاثة أجهزة**

### الخطوة 3.1 — حمّل وثبّت Docker Desktop

1. افتح المتصفح: **https://www.docker.com/products/docker-desktop/**
2. اضغط **Download for Windows**
3. شغّل الـ installer
4. في شاشة الإعداد تأكد من اختيار: ✅ **Use WSL 2 instead of Hyper-V**
5. بعد التثبيت أعد تشغيل الجهاز

### الخطوة 3.2 — اضبط Docker Desktop

بعد الـ Restart:
1. افتح Docker Desktop
2. اذهب لـ ⚙️ **Settings**
3. في **General**: تأكد أن ✅ **Use the WSL 2 based engine** مفعّل
4. في **Resources → WSL Integration**: فعّل Ubuntu-22.04 ✅
5. في **Resources → Advanced**:
   - **CPUs**: نص ما عندك على الأقل
   - **Memory**: 8 GB على الأقل (12 GB أفضل)
   - **Disk image size**: 80 GB
6. اضغط **Apply & Restart**

### الخطوة 3.3 — تحقق من Docker

```powershell
docker version
docker run --rm hello-world
```

---

## المرحلة 4 — فتح الـ Ports المطلوبة لـ Docker Swarm

> ⚠️ **على الثلاثة أجهزة — PowerShell كـ Administrator**

```powershell
# Port 2377 — Swarm management (Master فقط يحتاجه، لكن افتحه على الكل)
netsh advfirewall firewall add rule name="Swarm-2377" dir=in action=allow protocol=TCP localport=2377 remoteip=100.0.0.0/8

# Port 7946 — Node communication
netsh advfirewall firewall add rule name="Swarm-7946-TCP" dir=in action=allow protocol=TCP localport=7946 remoteip=100.0.0.0/8
netsh advfirewall firewall add rule name="Swarm-7946-UDP" dir=in action=allow protocol=UDP localport=7946 remoteip=100.0.0.0/8

# Port 4789 — Overlay network (VXLAN)
netsh advfirewall firewall add rule name="Swarm-4789-UDP" dir=in action=allow protocol=UDP localport=4789 remoteip=100.0.0.0/8

# تحقق
netsh advfirewall firewall show rule name="Swarm-2377"
```

> `100.0.0.0/8` هو نطاق Tailscale — يعني نسمح فقط للأجهزة الموجودة في الـ Tailnet.

---

## المرحلة 5 — إنشاء Docker Swarm

### الخطوة 5.1 — Init الـ Swarm على الـ MASTER فقط

```powershell
# ← استبدل 100.x.x.1 بـ Tailscale IP الـ Master الحقيقي
docker swarm init --advertise-addr 100.x.x.1
```

ستظهر رسالة زي دي — **احفظ الأمر الأخضر:**

```
Swarm initialized: current node (abcd1234) is now a manager.

To add a worker to this swarm, run the following command:

    docker swarm join --token SWMTKN-1-xxxxxxxxxxxxxxxxxx-yyyyyyyyyyyyyyyyyy 100.x.x.1:2377
```

### الخطوة 5.2 — لو نسيت الـ Token

```powershell
# على الـ Master
docker swarm join-token worker
```

### الخطوة 5.3 — أضف Worker 1 للـ Swarm

افتح PowerShell على **WORKER 1** والصق الأمر الذي حصلت عليه:

```powershell
# ← هذا مثال، استبدل بالـ Token الحقيقي
docker swarm join --token SWMTKN-1-xxxxxxxxxxxxxxxxxx-yyyyyyyyyyyyyyyyyy 100.x.x.1:2377
```

الناتج المتوقع:
```
This node joined a swarm as a worker.
```

### الخطوة 5.4 — أضف Worker 2 للـ Swarm

نفس الأمر على **WORKER 2**:

```powershell
docker swarm join --token SWMTKN-1-xxxxxxxxxxxxxxxxxx-yyyyyyyyyyyyyyyyyy 100.x.x.1:2377
```

### الخطوة 5.5 — تحقق من الـ Cluster على الـ Master

```powershell
docker node ls
```

المطلوب:

```
ID              HOSTNAME    STATUS    AVAILABILITY   MANAGER STATUS   ENGINE VERSION
xxxx *          master-pc   Ready     Active         Leader           26.x.x
yyyy            worker1-pc  Ready     Active                          26.x.x
zzzz            worker2-pc  Ready     Active                          26.x.x
```

الـ `*` بجانب الـ ID يعني ده الـ Node الحالي. الـ Master لازم يكون `Leader`.

### الخطوة 5.6 — أضف Labels للـ Nodes

```powershell
# على الـ Master فقط — استبدل الأسماء بالـ Hostnames الحقيقية

# Labels تحدد دور كل جهاز
docker node update --label-add role=master MASTER_HOSTNAME
docker node update --label-add role=worker WORKER1_HOSTNAME
docker node update --label-add role=worker WORKER2_HOSTNAME

# Labels تحدد أي Kafka Broker ينزل على أي جهاز
docker node update --label-add kafka.broker=1 MASTER_HOSTNAME
docker node update --label-add kafka.broker=2 WORKER1_HOSTNAME
docker node update --label-add kafka.broker=3 WORKER2_HOSTNAME

# تحقق
docker node inspect MASTER_HOSTNAME --format "{{ .Spec.Labels }}"
docker node inspect WORKER1_HOSTNAME --format "{{ .Spec.Labels }}"
docker node inspect WORKER2_HOSTNAME --format "{{ .Spec.Labels }}"
```

---

## المرحلة 6 — إنشاء الـ Overlay Network

```powershell
# على الـ Master فقط
docker network create `
    --driver overlay `
    --attachable `
    --subnet 10.20.0.0/16 `
    stocks-net

# تحقق
docker network ls | Select-String "stocks-net"
```

الناتج:
```
xxxxxxxxxxxx   stocks-net   overlay   swarm
```

---

## المرحلة 7 — إعداد الـ Shared Storage

البيانات (Kafka logs, MinIO, Postgres) لازم تتشارك بين الأجهزة عبر الشبكة.
سنستخدم **Windows SMB** (مشاركة ملفات Windows الطبيعية) فوق Tailscale.

### الخطوة 7.1 — أنشئ مجلدات البيانات على الـ Master

```powershell
# على الـ MASTER فقط — PowerShell عادي (مش Admin)
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

### الخطوة 7.2 — شارك المجلد عبر SMB على الـ Master

```powershell
# على الـ MASTER — PowerShell كـ Administrator
New-SmbShare `
    -Name "stocks-data" `
    -Path "C:\stocks-data" `
    -FullAccess "Everyone" `
    -Description "Shared storage for Docker Swarm cluster"

# تحقق
Get-SmbShare -Name "stocks-data"
```

### الخطوة 7.3 — افتح الـ Firewall للـ SMB على الـ Master

```powershell
# على الـ MASTER — PowerShell كـ Administrator
New-NetFirewallRule `
    -DisplayName "SMB for Tailscale" `
    -Direction Inbound `
    -Protocol TCP `
    -LocalPort 445 `
    -RemoteAddress "100.0.0.0/8" `
    -Action Allow
```

### الخطوة 7.4 — اختبر الوصول من الـ Workers

على **WORKER 1** وـ **WORKER 2**:

```powershell
# استبدل 100.x.x.1 بـ Tailscale IP للـ Master
Test-NetConnection -ComputerName 100.x.x.1 -Port 445
```

المطلوب: `TcpTestSucceeded : True`

### الخطوة 7.5 — أنشئ Docker Volumes على الثلاثة أجهزة

شغّل هذا الـ Script على **كل جهاز** (Master, Worker 1, Worker 2):

```powershell
# ← غيّر هذا بـ Tailscale IP للـ Master
$MASTER_IP = "100.x.x.1"

# ← غيّر بـ Windows Username وPassword للـ Master
$SMB_USER = "YOUR_MASTER_WINDOWS_USERNAME"
$SMB_PASS = "YOUR_MASTER_WINDOWS_PASSWORD"

$SMB_OPTS = "username=${SMB_USER},password=${SMB_PASS},vers=3.0"

docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/minio-data"       minio-data
docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/postgres-data"     postgres-data
docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/kafka-1-data"      kafka-1-data
docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/kafka-2-data"      kafka-2-data
docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/kafka-3-data"      kafka-3-data
docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/airflow-logs"      airflow-logs
docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/airflow-plugins"   airflow-plugins
docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/airflow-postgres"  airflow-postgres-data
docker volume create --driver local --opt type=cifs --opt o="$SMB_OPTS" --opt device="//${MASTER_IP}/stocks-data/registry"         registry-data

# تحقق
docker volume ls
```

> **ملاحظة بخصوص Username وPassword:** لو الـ Master ليس عليه password (تسجيل دخول بدون كلمة مرور)، ستحتاج لإنشاء كلمة مرور لحساب Windows أو إنشاء حساب جديد مخصص للمشاركة.

---

## المرحلة 8 — Registry محلي لـ Docker Images

### الخطوة 8.1 — شغّل Registry على الـ Master

```powershell
# على الـ MASTER فقط
docker run -d `
    --name registry `
    --restart always `
    -p 5000:5000 `
    -v registry-data:/var/lib/registry `
    registry:2

# تحقق
Invoke-WebRequest -Uri "http://localhost:5000/v2/_catalog" -UseBasicParsing | Select-Object Content
```

### الخطوة 8.2 — أضف الـ Registry كـ Insecure على الثلاثة أجهزة

على كل جهاز:
1. افتح Docker Desktop
2. اذهب لـ ⚙️ Settings → Docker Engine
3. في الـ JSON، أضف السطر الأحمر:

```json
{
  "builder": {"gc": {"defaultKeepStorage": "20GB", "enabled": true}},
  "experimental": false,
  "insecure-registries": ["100.x.x.1:5000"]
}
```

> استبدل `100.x.x.1` بـ Tailscale IP الـ Master

4. اضغط **Apply & Restart**

---

## المرحلة 9 — بناء Images ورفعها للـ Registry

### الخطوة 9.1 — على الـ MASTER فقط، روح لمجلد المشروع

```powershell
# استبدل بالمسار الحقيقي
cd C:\Users\YourName\Desktop\stocks-pipeline

# سجّل الـ Registry IP
$REG = "100.x.x.1:5000"
```

### الخطوة 9.2 — ابني Python Image

```powershell
docker build -t stocks-python:latest -f Dockerfile.python .
docker tag stocks-python:latest "${REG}/stocks-python:latest"
docker push "${REG}/stocks-python:latest"
```

### الخطوة 9.3 — ابني Spark Image

```powershell
# هذا يأخذ وقتاً (10-20 دقيقة) لأنه يحمّل JARs
docker build -t stocks-spark:3.5.7 -f spark\Dockerfile .\spark
docker tag stocks-spark:3.5.7 "${REG}/stocks-spark:3.5.7"
docker push "${REG}/stocks-spark:3.5.7"
```

### الخطوة 9.4 — ابني Airflow Image

```powershell
docker build -t stocks-airflow:2.10.4 -f airflow\Dockerfile .\airflow
docker tag stocks-airflow:2.10.4 "${REG}/stocks-airflow:2.10.4"
docker push "${REG}/stocks-airflow:2.10.4"
```

### الخطوة 9.5 — تحقق من الـ Images في الـ Registry

```powershell
Invoke-WebRequest -Uri "http://${REG}/v2/_catalog" -UseBasicParsing | Select-Object Content
```

المطلوب:
```json
{"repositories":["stocks-airflow","stocks-python","stocks-spark"]}
```

### الخطوة 9.6 — اسحب الـ Images على WORKER 1 وWORKER 2

```powershell
# على WORKER 1 وWORKER 2 — استبدل 100.x.x.1 بـ Master IP
$REG = "100.x.x.1:5000"

docker pull "${REG}/stocks-python:latest"
docker pull "${REG}/stocks-spark:3.5.7"
docker pull "${REG}/stocks-airflow:2.10.4"

# أعد التسمية بالأسماء الأصلية
docker tag "${REG}/stocks-python:latest" stocks-python:latest
docker tag "${REG}/stocks-spark:3.5.7" stocks-spark:3.5.7
docker tag "${REG}/stocks-airflow:2.10.4" stocks-airflow:2.10.4

# تحقق
docker images | Select-String "stocks"
```

---

## المرحلة 10 — ملف `.env`

احفظ الملف التالي في مجلد المشروع على الـ **Master** باسم `.env`:

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

لتوليد AIRFLOW_SECRET_KEY:

```powershell
python -c "import secrets; print(secrets.token_hex(32))"
```

---

## المرحلة 11 — ملف `docker-stack.yml`

احفظ الملف التالي في مجلد المشروع على الـ Master باسم `docker-stack.yml`.

**قبل الحفظ، استبدل هذه القيم:**

| ما تستبدله | بـ ماذا |
|---|---|
| `MASTER_TAILSCALE_IP` | Tailscale IP للـ Master (مثال: `100.64.0.1`) |
| `WORKER1_TAILSCALE_IP` | Tailscale IP لـ Worker 1 |
| `WORKER2_TAILSCALE_IP` | Tailscale IP لـ Worker 2 |
| `MASTER_HOSTNAME` | Hostname الـ Master من أمر `hostname` |
| `WORKER1_HOSTNAME` | Hostname Worker 1 |
| `WORKER2_HOSTNAME` | Hostname Worker 2 |
| `C:\YOUR\PROJECT\PATH` | المسار الكامل لمجلد المشروع على الـ Master |

```yaml
version: "3.9"

# ================================================================
# Docker Swarm Stack — Real-Time US Stocks Data Pipeline
# Layout:
#   Master  → kafka-1, spark master, minio, postgres, producer,
#              consumer, kafka-ui, pgadmin, airflow, kafka-init
#   Worker1 → kafka-2, spark-worker
#   Worker2 → kafka-3, spark-worker
# ================================================================

services:

  # ─────────────────────────────────────────────────────────────
  # KAFKA BROKERS
  # كل broker على جهاز مختلف — RF=3 يضمن إن البيانات موجودة
  # على الثلاثة أجهزة دائماً
  # ─────────────────────────────────────────────────────────────

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
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-1:29092,PLAINTEXT_HOST://MASTER_TAILSCALE_IP:9092
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
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-2:29093,PLAINTEXT_HOST://WORKER1_TAILSCALE_IP:9093
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
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-3:29094,PLAINTEXT_HOST://WORKER2_TAILSCALE_IP:9094
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

  # ─────────────────────────────────────────────────────────────
  # KAFKA INIT — ينشئ الـ Topics مرة واحدة ثم يخرج
  # ─────────────────────────────────────────────────────────────

  kafka-init:
    image: confluentinc/cp-kafka:7.6.1
    networks:
      - stocks-net
    entrypoint: ["/bin/bash", "-c"]
    command:
      - |
        set -e
        echo '--------------------------------------------------'
        echo 'Waiting 60s for all 3 brokers to elect leaders...'
        echo '--------------------------------------------------'
        sleep 60
        echo 'Creating topic: us-stocks-raw'
        kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists \
          --topic us-stocks-raw --partitions 6 --replication-factor 3 \
          --config retention.ms=604800000 \
          --config compression.type=lz4 \
          --config min.insync.replicas=2
        echo 'Creating topic: us-stocks-dead-letter'
        kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists \
          --topic us-stocks-dead-letter --partitions 3 --replication-factor 3 \
          --config retention.ms=2592000000 \
          --config compression.type=lz4
        echo 'All topics:'
        kafka-topics --bootstrap-server kafka-1:29092 --list
        echo 'Topics ready. Done.'
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.role == master
      restart_policy:
        condition: none

  # ─────────────────────────────────────────────────────────────
  # KAFKA UI
  # ─────────────────────────────────────────────────────────────

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

  # ─────────────────────────────────────────────────────────────
  # MINIO
  # ─────────────────────────────────────────────────────────────

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

  # ─────────────────────────────────────────────────────────────
  # POSTGRESQL
  # ─────────────────────────────────────────────────────────────

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

  # ─────────────────────────────────────────────────────────────
  # SPARK MASTER
  # ─────────────────────────────────────────────────────────────

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
        source: C:\YOUR\PROJECT\PATH\spark
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

  # ─────────────────────────────────────────────────────────────
  # SPARK WORKERS — واحد على كل Worker
  # ─────────────────────────────────────────────────────────────

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
        source: C:\YOUR\PROJECT\PATH\spark
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
        source: C:\YOUR\PROJECT\PATH\spark
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

  # ─────────────────────────────────────────────────────────────
  # PRODUCER & CONSUMER
  # ─────────────────────────────────────────────────────────────

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
        source: C:\YOUR\PROJECT\PATH\kafka
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
        source: C:\YOUR\PROJECT\PATH\kafka
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

  # ─────────────────────────────────────────────────────────────
  # PGADMIN
  # ─────────────────────────────────────────────────────────────

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

  # ─────────────────────────────────────────────────────────────
  # AIRFLOW
  # ─────────────────────────────────────────────────────────────

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
        source: C:\YOUR\PROJECT\PATH\dags
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
        source: C:\YOUR\PROJECT\PATH\dags
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
# NETWORKS
# ================================================================

networks:
  stocks-net:
    external: true

# ================================================================
# VOLUMES — كلها external (أنشأناها في مرحلة 7)
# ================================================================

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

## المرحلة 12 — Deploy الـ Stack

### الخطوة 12.1 — تأكد من كل حاجة قبل الـ Deploy

```powershell
# على الـ MASTER

# 1. كل الـ Nodes متصلة
docker node ls

# 2. الـ Network موجودة
docker network ls | Select-String stocks-net

# 3. الـ Volumes موجودة
docker volume ls | Select-String -Pattern "kafka|minio|postgres|airflow"

# 4. الـ Images موجودة
docker images | Select-String "stocks"

# 5. ملف .env موجود
Get-Content .env
```

### الخطوة 12.2 — Deploy

```powershell
# على الـ MASTER — في مجلد المشروع
cd C:\YOUR\PROJECT\PATH

docker stack deploy `
    --compose-file docker-stack.yml `
    --with-registry-auth `
    stocks
```

### الخطوة 12.3 — تابع الـ Deploy

```powershell
# شوف حالة كل الـ Services
docker stack services stocks

# شوف على أنهي جهاز كل container
docker stack ps stocks

# تابع بشكل مستمر (Ctrl+C للخروج)
while ($true) { Clear-Host; docker stack services stocks; Start-Sleep 5 }
```

---

## المرحلة 13 — التحقق النهائي

### الخطوة 13.1 — تحقق من Kafka

```powershell
# اعرف ID الـ kafka-1 container
$KAFKA1 = docker ps --filter "name=stocks_kafka-1" --format "{{.ID}}"

# تحقق من الـ Topics
docker exec $KAFKA1 kafka-topics `
    --bootstrap-server kafka-1:29092,kafka-2:29093,kafka-3:29094 `
    --list

# تحقق من الـ Brokers
docker exec $KAFKA1 kafka-broker-api-versions `
    --bootstrap-server kafka-1:29092,kafka-2:29093,kafka-3:29094
```

### الخطوة 13.2 — تحقق من Spark

افتح في المتصفح:
- **Spark Master UI:** `http://MASTER_TAILSCALE_IP:8081`
- يجب أن ترى Workers: 2 (واحد من Worker1، واحد من Worker2)

### الخطوة 13.3 — شغّل Spark Streaming Job

```powershell
# على الـ MASTER
$SPARK = docker ps --filter "name=stocks_spark" --format "{{.ID}}"

docker exec -it $SPARK `
    /opt/spark/bin/spark-submit `
    --master spark://spark:7077 `
    --conf spark.jars.ivy=/tmp/.ivy2 `
    /opt/spark/work-dir/streaming_job.py
```

### الخطوة 13.4 — شغّل Analytics Job

```powershell
$SPARK = docker ps --filter "name=stocks_spark" --format "{{.ID}}"

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
```

### الخطوة 13.5 — طبّق post_analytics.sql

```powershell
$PG = docker ps --filter "name=stocks_postgres" --format "{{.ID}}"

docker cp spark\post_analytics.sql ${PG}:/tmp/post_analytics.sql
docker exec $PG psql -U stocks -d stocks_analytics -f /tmp/post_analytics.sql
```

---

## 🌐 الـ URLs بعد الـ Deploy

| Service | URL | Login |
|---|---|---|
| Kafka UI | `http://MASTER_TAILSCALE_IP:8080` | لا يوجد |
| Spark Master UI | `http://MASTER_TAILSCALE_IP:8081` | لا يوجد |
| MinIO Console | `http://MASTER_TAILSCALE_IP:9001` | minioadmin / minioadmin |
| pgAdmin | `http://MASTER_TAILSCALE_IP:5050` | admin@stocks.com / admin123 |
| Airflow UI | `http://MASTER_TAILSCALE_IP:8082` | admin / admin |
| PostgreSQL | `MASTER_TAILSCALE_IP:5432` | stocks / stocks123 |

> يمكن الوصول لهذه الـ URLs من أي من الأجهزة الثلاثة عبر Tailscale.

---

## 🔧 أوامر مفيدة للمراقبة والإدارة

```powershell
# ── حالة الـ Cluster ────────────────────────────────────
# كل الـ Nodes وحالتها
docker node ls

# كل الـ Services وعدد الـ Replicas
docker stack services stocks

# كل الـ Containers وعلى أنهي جهاز
docker stack ps stocks

# الـ Containers الـ Failed أو المتوقفة
docker stack ps stocks --filter "desired-state=failed"

# ── Logs ────────────────────────────────────────────────
# Logs أي service (آخر 50 سطر)
docker service logs stocks_kafka-1 --tail 50

# Logs بشكل مستمر
docker service logs stocks_kafka-1 -f

# Logs الـ producer
docker service logs stocks_producer -f

# ── إدارة الـ Services ──────────────────────────────────
# أوقف service مؤقتاً
docker service scale stocks_consumer=0

# أعد تشغيل service
docker service scale stocks_consumer=1

# أعد تشغيل service بالقوة (force redeploy)
docker service update --force stocks_producer

# Update image لـ service
docker service update --image stocks-python:latest stocks_producer

# ── إيقاف كل شيء ────────────────────────────────────────
# إيقاف الـ Stack (الـ Volumes والبيانات تبقى)
docker stack rm stocks

# إعادة Deploy من الأول
docker stack deploy --compose-file docker-stack.yml --with-registry-auth stocks
```

---

## 🐛 Troubleshooting

### المشكلة: Worker لا يستطيع الانضمام للـ Swarm

```powershell
# على الـ Worker
# أولاً تحقق من الاتصال بـ Master
Test-NetConnection -ComputerName MASTER_TAILSCALE_IP -Port 2377

# لو فاشل، تحقق من الـ Firewall على الـ Master
netsh advfirewall firewall show rule name="Swarm-2377"

# لو مش موجود، أضفه على الـ Master
netsh advfirewall firewall add rule name="Swarm-2377" dir=in action=allow protocol=TCP localport=2377 remoteip=100.0.0.0/8

# على الـ Worker، جرّب مجدداً
docker swarm join --token TOKEN MASTER_TAILSCALE_IP:2377
```

### المشكلة: Service عالق في Pending

```powershell
# اعرف السبب
docker service ps stocks_SERVICENAME --no-trunc

# السبب الأكثر شيوعاً — Image مش موجودة على الـ Node
# على الـ Node المحدد
docker images | Select-String "stocks"

# لو مش موجودة، حمّلها
docker pull 100.x.x.1:5000/stocks-spark:3.5.7
docker tag 100.x.x.1:5000/stocks-spark:3.5.7 stocks-spark:3.5.7

# سبب آخر — Label غلط
docker node inspect WORKER1_HOSTNAME --format "{{ .Spec.Labels }}"
# أصلح الـ Labels
docker node update --label-add kafka.broker=2 WORKER1_HOSTNAME
```

### المشكلة: Kafka Brokers مش بيتواصلوا

```powershell
# تحقق من DNS داخل الـ Overlay Network
docker run --rm --network stocks-net alpine nslookup kafka-1
docker run --rm --network stocks-net alpine nslookup kafka-2
docker run --rm --network stocks-net alpine nslookup kafka-3

# تحقق من الـ Advertised Listeners في الـ docker-stack.yml
# تأكد إن كل broker يستخدم الـ Tailscale IP الصحيح
# kafka-1 → MASTER_TAILSCALE_IP
# kafka-2 → WORKER1_TAILSCALE_IP
# kafka-3 → WORKER2_TAILSCALE_IP
```

### المشكلة: SMB Volume لا يعمل

```powershell
# تحقق من الـ Share على الـ Master
Get-SmbShare -Name "stocks-data"

# اختبر الوصول من الـ Worker
Test-NetConnection -ComputerName MASTER_TAILSCALE_IP -Port 445

# جرّب Mount يدوي
net use Z: \\MASTER_TAILSCALE_IP\stocks-data /user:MASTER_USERNAME MASTER_PASSWORD

# لو في مشكلة credentials في Docker Volume
# أضف الـ Credentials لـ Windows Credential Manager
cmdkey /add:MASTER_TAILSCALE_IP /user:MASTER_USERNAME /pass:MASTER_PASSWORD
```

### المشكلة: Docker Desktop لا يبدأ

```powershell
# تحقق من WSL2
wsl --list --verbose

# أعد تشغيل WSL
wsl --shutdown
wsl -d Ubuntu-22.04

# أعد تشغيل Docker service
net stop com.docker.service
net start com.docker.service
```

### المشكلة: Tailscale يفقد الاتصال

```powershell
# تحقق من حالة Tailscale
& "C:\Program Files\Tailscale\tailscale.exe" status

# أعد الاتصال
& "C:\Program Files\Tailscale\tailscale.exe" down
& "C:\Program Files\Tailscale\tailscale.exe" up
```

---

## 📊 جدول توزيع الـ Services

| Service | Master | Worker 1 | Worker 2 |
|---|---|---|---|
| kafka-1 | ✅ | | |
| kafka-2 | | ✅ | |
| kafka-3 | | | ✅ |
| kafka-init | ✅ | | |
| kafka-ui | ✅ | | |
| minio | ✅ | | |
| minio-init | ✅ | | |
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

*تاريخ الإنشاء: September 2026 | Real-Time US Stocks Data Pipeline*
