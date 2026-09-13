# 🌐 Distributed Pipeline Cluster — High Availability (HA) على Windows
## Real-Time US Stocks Data Pipeline | 3 أجهزة Windows بـ Docker Swarm + Tailscale
### كل جهاز يحمل كل شيء — الـ Failover تلقائي

---

> **الهدف من هذا الدليل:** كل جهاز من الثلاثة يشغّل نسخة كاملة من كل service. لو الـ Master وقع، أحد الـ Workers يأخذ دوره تلقائياً بدون تدخل.

---

## 📋 جدول المحتويات

1. [المعمارية — HA كاملة](#1-المعمارية)
2. [متطلبات الأجهزة والبرامج](#2-المتطلبات)
3. [المرحلة 1 — تثبيت Tailscale على Windows](#3-مرحلة-1--tailscale)
4. [المرحلة 2 — تثبيت Docker Desktop على Windows](#4-مرحلة-2--docker-desktop)
5. [المرحلة 3 — إنشاء Swarm Cluster بـ 3 Managers](#5-مرحلة-3--swarm-cluster)
6. [المرحلة 4 — إعداد Shared Storage بـ NFS على Windows](#6-مرحلة-4--shared-storage)
7. [المرحلة 5 — بناء Docker Images ورفعها لـ Registry](#7-مرحلة-5--docker-images)
8. [المرحلة 6 — ملف docker-stack.yml للـ HA الكاملة](#8-مرحلة-6--docker-stackyml)
9. [المرحلة 7 — Deploy الـ Stack](#9-مرحلة-7--deploy)
10. [المرحلة 8 — اختبار الـ Failover](#10-مرحلة-8--اختبار-الـ-failover)
11. [الـ URLs والـ Ports بعد الـ Deploy](#11-الـ-urls-والـ-ports)
12. [Troubleshooting شامل لـ Windows](#12-troubleshooting)

---

## 1. المعمارية

### مبدأ HA في هذا الإعداد

```
┌────────────────────────────────────────────────────────────────────────┐
│                    TAILSCALE VPN MESH (100.x.x.x)                     │
│                                                                        │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────────┐    │
│  │    NODE 1        │  │    NODE 2        │  │    NODE 3         │    │
│  │  (PC الأول)      │  │  (PC الثاني)     │  │  (PC الثالث)      │    │
│  │  Swarm Manager   │  │  Swarm Manager   │  │  Swarm Manager    │    │
│  │                  │  │                  │  │                   │    │
│  │ ✅ kafka-1       │  │ ✅ kafka-2       │  │ ✅ kafka-3        │    │
│  │ ✅ kafka-init    │  │                  │  │                   │    │
│  │ ✅ minio         │  │ ✅ minio         │  │ ✅ minio          │    │
│  │ ✅ postgres      │  │ ✅ postgres      │  │ ✅ postgres        │    │
│  │    (primary)     │  │    (standby)     │  │    (standby)      │    │
│  │ ✅ spark-master  │  │ ✅ spark-worker  │  │ ✅ spark-worker   │    │
│  │ ✅ producer      │  │                  │  │                   │    │
│  │ ✅ consumer      │  │                  │  │                   │    │
│  │ ✅ kafka-ui      │  │ ✅ kafka-ui      │  │ ✅ kafka-ui       │    │
│  │ ✅ pgadmin       │  │                  │  │                   │    │
│  │ ✅ airflow       │  │                  │  │                   │    │
│  └──────────────────┘  └──────────────────┘  └───────────────────┘    │
│                                                                        │
│   كل جهاز Manager ← لو NODE 1 وقع، NODE 2 يصبح Leader تلقائياً      │
└────────────────────────────────────────────────────────────────────────┘
```

### الفرق عن الإعداد العادي

| الإعداد العادي | الإعداد الحالي (HA) |
|---|---|
| Master = Manager وحيد | كل الأجهزة الثلاثة = Managers |
| لو الـ Master وقع، الـ Cluster مات | لو جهاز وقع، التانيين يكملوا |
| Kafka RF=3 لكن Manager واحد | Kafka RF=3 + Swarm Quorum يحتاج 2 من 3 |
| PostgreSQL على Master بس | PostgreSQL primary على NODE1، standby على NODE2/3 |

> **ملاحظة Quorum:** Docker Swarm يحتاج أغلبية الـ Managers تكون متصلة. مع 3 Managers، تقدر يوقع جهاز واحد وكل حاجة تكمل. لو وقع 2، الـ Cluster يتوقف عن قبول تغييرات (لكن الـ Containers الشغّالة تكمل).

---

## 2. المتطلبات

### متطلبات كل جهاز

| المتطلب | الحد الأدنى | الموصى به |
|---|---|---|
| Windows | Windows 10 Pro/Enterprise (Build 19041+) أو Windows 11 | Windows 11 |
| RAM | 8 GB | 16 GB |
| CPU | 4 Cores | 6+ Cores |
| Disk | 80 GB | 150 GB SSD |
| Network | أي اتصال بالإنترنت | Stable Broadband |

> **مهم جداً:** Windows 10 Home **لا يدعم** Hyper-V وبالتالي لا يدعم Docker Desktop بالشكل الكامل. الحل: استخدم WSL2 backend فقط.

### البرامج المطلوبة على كل جهاز

- [Tailscale for Windows](https://tailscale.com/download/windows) — VPN Mesh
- [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/) — مع WSL2 Backend
- [Windows Subsystem for Linux 2 (WSL2)](https://learn.microsoft.com/en-us/windows/wsl/install) — Ubuntu 22.04

### معلومات تحتاجها قبل البدء

على كل جهاز شغّل هذا الأمر في PowerShell وسجّل النتيجة:

```powershell
hostname
```

سنستخدم في هذا الدليل:
- `PC1` = الجهاز الأول (NODE 1)
- `PC2` = الجهاز الثاني (NODE 2)
- `PC3` = الجهاز الثالث (NODE 3)

---

## 3. مرحلة 1 — Tailscale

> **اعمل الخطوات دي على الثلاثة أجهزة.**

### الخطوة 3.1 — تثبيت Tailscale

1. افتح المتصفح وروح على: https://tailscale.com/download/windows
2. حمّل الـ installer وشغّله
3. اتبع الخطوات العادية للتثبيت
4. بعد التثبيت، Tailscale هيظهر في الـ System Tray (ساعة Windows)

### الخطوة 3.2 — تسجيل الدخول

1. على كل جهاز، اضغط بيمين على Tailscale icon في الـ System Tray
2. اختر **"Log in"**
3. في المتصفح، سجّل دخول بـ Google أو GitHub أو Microsoft
4. **استخدم نفس الـ Account على الأجهزة الثلاثة**

### الخطوة 3.3 — اعرف الـ Tailscale IP لكل جهاز

على كل جهاز، افتح PowerShell وشغّل:

```powershell
# طريقة 1 — من PowerShell
(Get-NetIPAddress -AddressFamily IPv4 | Where-Object {$_.IPAddress -like "100.*"}).IPAddress
```

أو:

```powershell
# طريقة 2 — من Tailscale تنفسه
& "C:\Program Files\Tailscale\tailscale.exe" ip
```

**سجّل النتائج هنا (استبدل بالـ IPs الحقيقية):**

```
NODE1_IP=100.x.x.1   ← PC1
NODE2_IP=100.x.x.2   ← PC2
NODE3_IP=100.x.x.3   ← PC3
```

### الخطوة 3.4 — اختبر الاتصال بين الأجهزة

على PC1، شغّل في PowerShell:

```powershell
# استبدل بالـ IPs الحقيقية
ping 100.x.x.2 -n 4
ping 100.x.x.3 -n 4
```

على PC2:

```powershell
ping 100.x.x.1 -n 4
ping 100.x.x.3 -n 4
```

**لازم كل الـ pings تنجح قبل ما تكمل.**

### الخطوة 3.5 — تحقق إن Tailscale شغّال

```powershell
# من أي جهاز
& "C:\Program Files\Tailscale\tailscale.exe" status
```

الناتج المتوقع:

```
100.x.x.1  PC1  your-account@  windows  -
100.x.x.2  PC2  your-account@  windows  -
100.x.x.3  PC3  your-account@  windows  -
```

---

## 4. مرحلة 2 — Docker Desktop

> **اعمل الخطوات دي على الثلاثة أجهزة.**

### الخطوة 4.1 — تفعيل WSL2

افتح PowerShell كـ Administrator على كل جهاز:

```powershell
# تفعيل WSL و Virtual Machine Platform
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart

# أعد تشغيل الجهاز
Restart-Computer
```

بعد الـ Restart، افتح PowerShell كـ Administrator مرة ثانية:

```powershell
# اجعل WSL2 هو الإصدار الافتراضي
wsl --set-default-version 2

# ثبّت Ubuntu 22.04
wsl --install -d Ubuntu-22.04

# انتظر حتى ينتهي التثبيت ويطلب منك Username وPassword
# أدخل اسم مستخدم (مثلاً: stocks) وكلمة مرور
```

### الخطوة 4.2 — تثبيت Docker Desktop

1. افتح المتصفح وروح على: https://www.docker.com/products/docker-desktop/
2. حمّل **"Docker Desktop for Windows"**
3. شغّل الـ installer
4. في خطوة الـ Configuration، تأكد من اختيار:
   - ✅ **"Use WSL 2 instead of Hyper-V"**
   - ✅ **"Add shortcut to desktop"**
5. بعد التثبيت، أعد تشغيل الجهاز

### الخطوة 4.3 — إعداد Docker Desktop

بعد الـ Restart:

1. افتح Docker Desktop
2. اذهب لـ **Settings** (⚙️ icon)
3. في **General**:
   - ✅ تأكد من تفعيل **"Use the WSL 2 based engine"**
4. في **Resources → WSL Integration**:
   - ✅ فعّل الـ Integration مع Ubuntu-22.04
5. اضغط **"Apply & Restart"**

### الخطوة 4.4 — زيادة موارد Docker (مهم جداً)

في Docker Desktop → Settings → Resources:

| الإعداد | القيمة الموصى بها |
|---|---|
| CPUs | كل الـ CPUs المتاحة (أو نصها على الأقل) |
| Memory | 8 GB على الأقل (12 GB أفضل) |
| Swap | 2 GB |
| Disk image size | 80 GB على الأقل |

اضغط **"Apply & Restart"**

### الخطوة 4.5 — اختبر Docker

افتح PowerShell العادي (مش Administrator):

```powershell
docker version
docker compose version
docker run --rm hello-world
```

لازم يشتغل بدون أخطاء.

### الخطوة 4.6 — إعداد Firewall لـ Docker Swarm

Docker Swarm يحتاج ports معينة. لأن الاتصال عبر Tailscale، نفتح هذه الـ Ports على الـ Tailscale Interface فقط.

افتح PowerShell كـ Administrator على كل جهاز:

```powershell
# اعرف اسم الـ Tailscale Network Adapter
Get-NetAdapter | Where-Object {$_.InterfaceDescription -like "*Tailscale*"}
# عادةً اسمه "Tailscale"

# افتح الـ Ports المطلوبة لـ Docker Swarm
# Port 2377 — Swarm cluster management
netsh advfirewall firewall add rule name="Docker Swarm Port 2377" dir=in action=allow protocol=TCP localport=2377 remoteip=100.0.0.0/8

# Port 7946 — Node Communication (TCP + UDP)
netsh advfirewall firewall add rule name="Docker Swarm Port 7946 TCP" dir=in action=allow protocol=TCP localport=7946 remoteip=100.0.0.0/8
netsh advfirewall firewall add rule name="Docker Swarm Port 7946 UDP" dir=in action=allow protocol=UDP localport=7946 remoteip=100.0.0.0/8

# Port 4789 — Overlay Network (VXLAN)
netsh advfirewall firewall add rule name="Docker Swarm Port 4789 UDP" dir=in action=allow protocol=UDP localport=4789 remoteip=100.0.0.0/8

# تحقق
netsh advfirewall firewall show rule name="Docker Swarm Port 2377"
```

> **ملاحظة:** الـ `100.0.0.0/8` هو نطاق Tailscale IPs. هذا يعني إننا نسمح فقط للأجهزة الموصولة بـ Tailscale.

---

## 5. مرحلة 3 — Swarm Cluster

> **في هذا الإعداد HA، الثلاثة أجهزة كلهم Managers** — مش Manager و Workers. هذا يضمن إن الـ Cluster يكمل حتى لو وقع جهازان (لن يكمل، لكن إذا وقع جهاز واحد فقط، التانيين يكملوا).

### الخطوة 5.1 — Initialize الـ Swarm على PC1

افتح PowerShell على PC1:

```powershell
# استبدل 100.x.x.1 بـ Tailscale IP الحقيقي لـ PC1
docker swarm init --advertise-addr 100.x.x.1
```

الناتج:

```
Swarm initialized: current node (XXXXXXXX) is now a manager.

To add a worker to this swarm, run the following command:
    docker swarm join --token SWMTKN-1-xxxx-yyyy 100.x.x.1:2377

To add a manager to this swarm, run 'docker swarm join-token manager' and follow the instructions.
```

### الخطوة 5.2 — احصل على Manager Token

```powershell
# على PC1 فقط
# نحتاج Manager token (مش Worker token) عشان كل الأجهزة تكون Managers
docker swarm join-token manager
```

الناتج:

```
To add a manager to this swarm, run the following command:

    docker swarm join --token SWMTKN-1-MANAGER-TOKEN-HERE 100.x.x.1:2377
```

**احفظ هذا الأمر — ستحتاجه للخطوات التالية.**

### الخطوة 5.3 — أضف PC2 كـ Manager

افتح PowerShell على PC2 والصق الأمر الذي حصلت عليه:

```powershell
# على PC2 فقط — استبدل بالـ Token الحقيقي
docker swarm join \
    --token SWMTKN-1-MANAGER-TOKEN-HERE \
    100.x.x.1:2377
```

الناتج:

```
This node joined a swarm as a manager.
```

### الخطوة 5.4 — أضف PC3 كـ Manager

افتح PowerShell على PC3:

```powershell
# على PC3 فقط — نفس الأمر
docker swarm join \
    --token SWMTKN-1-MANAGER-TOKEN-HERE \
    100.x.x.1:2377
```

### الخطوة 5.5 — تحقق من الـ Cluster

على أي جهاز:

```powershell
docker node ls
```

الناتج المتوقع:

```
ID                HOSTNAME   STATUS    AVAILABILITY   MANAGER STATUS   ENGINE VERSION
XXXX *            PC1        Ready     Active         Leader           26.x.x
YYYY              PC2        Ready     Active         Reachable        26.x.x
ZZZZ              PC3        Ready     Active         Reachable        26.x.x
```

**الثلاثة لازم يكونوا `Ready` و `Active`. PC1 هو `Leader` والتانيين `Reachable`.**

> **لو وقع PC1:** أحد PC2 أو PC3 سيصبح Leader تلقائياً.

### الخطوة 5.6 — أضف Labels للـ Nodes

```powershell
# على أي Manager (PC1)

# Labels لتحديد كل جهاز
docker node update --label-add nodeid=node1 PC1
docker node update --label-add nodeid=node2 PC2
docker node update --label-add nodeid=node3 PC3

# Labels للـ Kafka (كل جهاز يشغّل broker مختلف)
docker node update --label-add kafka.broker=1 PC1
docker node update --label-add kafka.broker=2 PC2
docker node update --label-add kafka.broker=3 PC3

# Labels للـ Postgres (node1 هو primary)
docker node update --label-add postgres.role=primary PC1
docker node update --label-add postgres.role=standby PC2
docker node update --label-add postgres.role=standby PC3

# تحقق
docker node inspect PC1 --format '{{ .Spec.Labels }}'
```

### الخطوة 5.7 — أنشئ الـ Overlay Network

```powershell
# على PC1
docker network create \
    --driver overlay \
    --attachable \
    --subnet 10.20.0.0/16 \
    stocks-net
```

---

## 6. مرحلة 4 — Shared Storage

لأن Windows لا يدعم NFS Server مباشرة بسهولة، سنستخدم **Samba (SMB/CIFS)** — وهو بروتوكول مشاركة ملفات Windows الطبيعي.

### الفكرة الكاملة

```
PC1 (SMB Server)
    └── C:\stocks-data\
           ├── minio-data\
           ├── postgres-data\
           ├── kafka-1-data\
           ├── kafka-2-data\
           └── kafka-3-data\

PC2 وPC3 يعملوا Mount للمجلدات دي عبر Tailscale
```

### الخطوة 6.1 — إعداد SMB Share على PC1

افتح PowerShell كـ Administrator على PC1:

```powershell
# أنشئ المجلدات
New-Item -ItemType Directory -Force -Path "C:\stocks-data\minio-data"
New-Item -ItemType Directory -Force -Path "C:\stocks-data\postgres-data"
New-Item -ItemType Directory -Force -Path "C:\stocks-data\kafka-1-data"
New-Item -ItemType Directory -Force -Path "C:\stocks-data\kafka-2-data"
New-Item -ItemType Directory -Force -Path "C:\stocks-data\kafka-3-data"
New-Item -ItemType Directory -Force -Path "C:\stocks-data\airflow-logs"
New-Item -ItemType Directory -Force -Path "C:\stocks-data\airflow-plugins"
New-Item -ItemType Directory -Force -Path "C:\stocks-data\airflow-postgres-data"

# أنشئ Share واحد يشمل كل المجلدات
New-SmbShare -Name "stocks-data" `
    -Path "C:\stocks-data" `
    -FullAccess "Everyone" `
    -Description "Docker Swarm Shared Storage for Stocks Pipeline"

# تحقق
Get-SmbShare -Name "stocks-data"
```

### الخطوة 6.2 — افتح الـ Firewall للـ SMB على Tailscale فقط

```powershell
# على PC1 — اسمح بـ SMB من Tailscale IPs فقط
New-NetFirewallRule `
    -DisplayName "Samba for Tailscale" `
    -Direction Inbound `
    -Protocol TCP `
    -LocalPort 445 `
    -RemoteAddress "100.0.0.0/8" `
    -Action Allow

New-NetFirewallRule `
    -DisplayName "Samba NetBIOS for Tailscale" `
    -Direction Inbound `
    -Protocol TCP `
    -LocalPort 139 `
    -RemoteAddress "100.0.0.0/8" `
    -Action Allow
```

### الخطوة 6.3 — اختبر الوصول من PC2 وPC3

على PC2 وPC3، افتح PowerShell:

```powershell
# استبدل 100.x.x.1 بـ Tailscale IP لـ PC1
Test-NetConnection -ComputerName 100.x.x.1 -Port 445

# لو نجح، جرّب تعرض المجلدات
net view \\100.x.x.1\stocks-data
```

### الخطوة 6.4 — أنشئ Docker NFS Volumes على كل الأجهزة

> هذه الطريقة تستخدم Docker Volume مع SMB/CIFS driver.

**أولاً، ثبّت الـ Volume Plugin على كل الأجهزة الثلاثة:**

افتح PowerShell على كل جهاز:

```powershell
# ثبّت SMB volume plugin
docker plugin install --grant-all-permissions vieux/sshfs || true
# الـ plugin ده مش مناسب لـ SMB، هنستخدم طريقة أبسط
```

**الطريقة الأبسط لـ Windows — استخدام Mount Points في WSL2:**

على كل جهاز (PC1 وPC2 وPC3)، افتح **Ubuntu WSL2** من Start Menu وشغّل:

```bash
# على PC1 — أنشئ مجلد المشاركة
sudo mkdir -p /mnt/stocks-data

# على PC2 وPC3 — Mount الـ SMB Share من PC1
# استبدل 100.x.x.1 بـ Tailscale IP لـ PC1
# استبدل YOUR_PC1_USERNAME بـ username الـ Windows على PC1

sudo apt-get install -y cifs-utils

sudo mkdir -p /mnt/stocks-data

sudo mount -t cifs //100.x.x.1/stocks-data /mnt/stocks-data \
    -o username=YOUR_PC1_USERNAME,password=YOUR_PC1_PASSWORD,uid=1000,gid=1000,vers=3.0

# تحقق
ls /mnt/stocks-data
```

**لجعل الـ Mount دائماً (عند بدء WSL2):**

```bash
# على PC2 وPC3 — أضف للـ /etc/fstab داخل WSL2
echo "//100.x.x.1/stocks-data /mnt/stocks-data cifs username=PC1_USER,password=PC1_PASS,uid=1000,gid=1000,vers=3.0,_netdev 0 0" | sudo tee -a /etc/fstab

# اختبر
sudo mount -a
```

### الخطوة 6.5 — أنشئ Docker Volumes على كل الأجهزة الثلاثة

شغّل هذا في PowerShell على **كل جهاز**:

```powershell
# PC1_TAILSCALE_IP = Tailscale IP لـ PC1
$PC1_IP = "100.x.x.1"   # ← غيّر هذا

# أنشئ الـ Volumes
docker volume create --driver local `
    --opt type=cifs `
    --opt o="username=PC1_USER,password=PC1_PASS,vers=3.0" `
    --opt device="//$PC1_IP/stocks-data/minio-data" `
    minio-data

docker volume create --driver local `
    --opt type=cifs `
    --opt o="username=PC1_USER,password=PC1_PASS,vers=3.0" `
    --opt device="//$PC1_IP/stocks-data/postgres-data" `
    postgres-data

docker volume create --driver local `
    --opt type=cifs `
    --opt o="username=PC1_USER,password=PC1_PASS,vers=3.0" `
    --opt device="//$PC1_IP/stocks-data/kafka-1-data" `
    kafka-1-data

docker volume create --driver local `
    --opt type=cifs `
    --opt o="username=PC1_USER,password=PC1_PASS,vers=3.0" `
    --opt device="//$PC1_IP/stocks-data/kafka-2-data" `
    kafka-2-data

docker volume create --driver local `
    --opt type=cifs `
    --opt o="username=PC1_USER,password=PC1_PASS,vers=3.0" `
    --opt device="//$PC1_IP/stocks-data/kafka-3-data" `
    kafka-3-data

docker volume create --driver local `
    --opt type=cifs `
    --opt o="username=PC1_USER,password=PC1_PASS,vers=3.0" `
    --opt device="//$PC1_IP/stocks-data/airflow-logs" `
    airflow-logs

docker volume create --driver local `
    --opt type=cifs `
    --opt o="username=PC1_USER,password=PC1_PASS,vers=3.0" `
    --opt device="//$PC1_IP/stocks-data/airflow-plugins" `
    airflow-plugins

docker volume create --driver local `
    --opt type=cifs `
    --opt o="username=PC1_USER,password=PC1_PASS,vers=3.0" `
    --opt device="//$PC1_IP/stocks-data/airflow-postgres-data" `
    airflow-postgres-data

# تحقق
docker volume ls
```

---

## 7. مرحلة 5 — Docker Images

### الخطوة 7.1 — أنشئ Registry محلي على PC1

```powershell
# على PC1
docker run -d `
    -p 5000:5000 `
    --name registry `
    --restart always `
    -v "C:\stocks-data\registry:/var/lib/registry" `
    registry:2

# تحقق
Invoke-WebRequest -Uri "http://localhost:5000/v2/_catalog" -UseBasicParsing
```

### الخطوة 7.2 — أضف الـ Registry كـ Insecure على كل الأجهزة

على كل جهاز، افتح Docker Desktop → Settings → Docker Engine، وعدّل الـ JSON:

```json
{
  "builder": {"gc": {"defaultKeepStorage": "20GB", "enabled": true}},
  "experimental": false,
  "insecure-registries": ["100.x.x.1:5000"]
}
```

> استبدل `100.x.x.1` بـ Tailscale IP لـ PC1

اضغط **"Apply & Restart"**

### الخطوة 7.3 — ابني الـ Images على PC1

افتح PowerShell وروح لمجلد المشروع:

```powershell
# غيّر المسار لمجلد المشروع الفعلي
cd C:\path\to\your\project

$REGISTRY = "100.x.x.1:5000"   # ← غيّر بـ Tailscale IP لـ PC1

# 1. Python Image (Producer + Consumer)
docker build -t stocks-python:latest -f Dockerfile.python .
docker tag stocks-python:latest "${REGISTRY}/stocks-python:latest"
docker push "${REGISTRY}/stocks-python:latest"

# 2. Spark Image (قد يأخذ وقتاً طويلاً بسبب تحميل JARs)
docker build -t stocks-spark:3.5.7 -f spark\Dockerfile .\spark
docker tag stocks-spark:3.5.7 "${REGISTRY}/stocks-spark:3.5.7"
docker push "${REGISTRY}/stocks-spark:3.5.7"

# 3. Airflow Image
docker build -t stocks-airflow:2.10.4 -f airflow\Dockerfile .\airflow
docker tag stocks-airflow:2.10.4 "${REGISTRY}/stocks-airflow:2.10.4"
docker push "${REGISTRY}/stocks-airflow:2.10.4"

# تحقق من الـ Registry
Invoke-WebRequest -Uri "http://${REGISTRY}/v2/_catalog" -UseBasicParsing | Select-Object -ExpandProperty Content
```

### الخطوة 7.4 — اسحب الـ Images على PC2 وPC3

```powershell
# على PC2 وPC3 — استبدل 100.x.x.1 بـ Tailscale IP لـ PC1
$REGISTRY = "100.x.x.1:5000"

docker pull "${REGISTRY}/stocks-python:latest"
docker pull "${REGISTRY}/stocks-spark:3.5.7"
docker pull "${REGISTRY}/stocks-airflow:2.10.4"

# أعد التسمية
docker tag "${REGISTRY}/stocks-python:latest" stocks-python:latest
docker tag "${REGISTRY}/stocks-spark:3.5.7" stocks-spark:3.5.7
docker tag "${REGISTRY}/stocks-airflow:2.10.4" stocks-airflow:2.10.4

# تحقق
docker images | Select-String "stocks"
```

---

## 8. مرحلة 6 — docker-stack.yml

احفظ الملف التالي في مجلد المشروع على PC1 باسم `docker-stack.yml`:

> **مهم:** استبدل كل `100.x.x.1`، `100.x.x.2`، `100.x.x.3` بالـ IPs الحقيقية. استبدل `PC1`، `PC2`، `PC3` بالـ Hostnames الحقيقية.

```yaml
# docker-stack.yml — HA Setup: كل الأجهزة تشغّل كل شيء
# Docker Swarm Stack — Real-Time US Stocks Data Pipeline
# 3 Managers + Distributed Services

version: "3.9"

services:

  # ════════════════════════════════════════════════════════
  # KAFKA — كل broker على جهاز مختلف
  # ════════════════════════════════════════════════════════

  kafka-1:
    image: confluentinc/cp-kafka:7.6.1
    networks:
      - stocks-net
    ports:
      - target: 9092
        published: 9092
        mode: host
    environment:
      KAFKA_NODE_ID:            1
      CLUSTER_ID:               MkU3OEVBNTcwNTJENDM2Qk
      KAFKA_PROCESS_ROLES:      broker,controller
      KAFKA_LISTENERS:          CONTROLLER://0.0.0.0:29091,PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092
      # 100.x.x.1 = Tailscale IP لـ PC1
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-1:29092,PLAINTEXT_HOST://100.x.x.1:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME:     PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES:      CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS:       1@kafka-1:29091,2@kafka-2:29091,3@kafka-3:29091
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:         3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR:            2
      KAFKA_DEFAULT_REPLICATION_FACTOR:               3
      KAFKA_MIN_INSYNC_REPLICAS:                      2
      KAFKA_NUM_PARTITIONS:                           6
      KAFKA_AUTO_CREATE_TOPICS_ENABLE:                "false"
      KAFKA_LOG_DIRS:                                 /var/lib/kafka/data
    volumes:
      - kafka-1-data:/var/lib/kafka/data
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          # Kafka-1 دائماً على PC1 (node1)
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
      KAFKA_NODE_ID:            2
      CLUSTER_ID:               MkU3OEVBNTcwNTJENDM2Qk
      KAFKA_PROCESS_ROLES:      broker,controller
      KAFKA_LISTENERS:          CONTROLLER://0.0.0.0:29091,PLAINTEXT://0.0.0.0:29093,PLAINTEXT_HOST://0.0.0.0:9093
      # 100.x.x.2 = Tailscale IP لـ PC2
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-2:29093,PLAINTEXT_HOST://100.x.x.2:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME:     PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES:      CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS:       1@kafka-1:29091,2@kafka-2:29091,3@kafka-3:29091
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:         3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR:            2
      KAFKA_DEFAULT_REPLICATION_FACTOR:               3
      KAFKA_MIN_INSYNC_REPLICAS:                      2
      KAFKA_NUM_PARTITIONS:                           6
      KAFKA_AUTO_CREATE_TOPICS_ENABLE:                "false"
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
      KAFKA_NODE_ID:            3
      CLUSTER_ID:               MkU3OEVBNTcwNTJENDM2Qk
      KAFKA_PROCESS_ROLES:      broker,controller
      KAFKA_LISTENERS:          CONTROLLER://0.0.0.0:29091,PLAINTEXT://0.0.0.0:29094,PLAINTEXT_HOST://0.0.0.0:9094
      # 100.x.x.3 = Tailscale IP لـ PC3
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-3:29094,PLAINTEXT_HOST://100.x.x.3:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME:     PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES:      CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS:       1@kafka-1:29091,2@kafka-2:29091,3@kafka-3:29091
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:         3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR:            2
      KAFKA_DEFAULT_REPLICATION_FACTOR:               3
      KAFKA_MIN_INSYNC_REPLICAS:                      2
      KAFKA_NUM_PARTITIONS:                           6
      KAFKA_AUTO_CREATE_TOPICS_ENABLE:                "false"
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

  kafka-init:
    image: confluentinc/cp-kafka:7.6.1
    networks:
      - stocks-net
    entrypoint: ["/bin/bash", "-c"]
    command:
      - |
        set -e
        echo 'Waiting 60s for all Kafka brokers to be ready...'
        sleep 60
        echo 'Creating topics...'
        kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists \
          --topic us-stocks-raw --partitions 6 --replication-factor 3 \
          --config retention.ms=604800000 \
          --config compression.type=lz4 \
          --config min.insync.replicas=2
        kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists \
          --topic us-stocks-dead-letter --partitions 3 --replication-factor 3 \
          --config retention.ms=2592000000 \
          --config compression.type=lz4
        echo 'Topics:'
        kafka-topics --bootstrap-server kafka-1:29092 --list
        echo 'Done.'
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.nodeid == node1
      restart_policy:
        condition: none

  # ════════════════════════════════════════════════════════
  # KAFKA UI — على كل الأجهزة (replicas=3)
  # ════════════════════════════════════════════════════════

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
      KAFKA_CLUSTERS_0_AUDIT_TOPICAUDITENABLED: "false"
      DYNAMIC_CONFIG_ENABLED: "true"
    deploy:
      mode: global    # نسخة على كل جهاز
      restart_policy:
        condition: on-failure
        delay: 10s

  # ════════════════════════════════════════════════════════
  # MINIO — على كل الأجهزة للـ Read Access (Primary على PC1)
  # ════════════════════════════════════════════════════════
  # ملاحظة: MinIO في هذا الإعداد يكتب على PC1 فقط عبر الـ Shared Volume
  # الـ replicas على PC2/PC3 تقرأ من نفس الـ Volume

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
          - node.labels.nodeid == node1
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
        mc alias set local http://minio:9000 "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"
        mc mb --ignore-existing local/stocks
        echo 'Bucket stocks is ready.'
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.nodeid == node1
      restart_policy:
        condition: none

  # ════════════════════════════════════════════════════════
  # POSTGRESQL — Primary على PC1
  # Standby نسخة احتياطية على PC2 (يدوياً عند الحاجة)
  # ════════════════════════════════════════════════════════

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
          - node.labels.postgres.role == primary
      restart_policy:
        condition: on-failure
        delay: 15s
        max_attempts: 10

  # ════════════════════════════════════════════════════════
  # SPARK — Master على PC1، Workers على كل الأجهزة
  # ════════════════════════════════════════════════════════

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
        source: C:\path\to\your\project\spark
        target: /opt/spark/work-dir
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.nodeid == node1
      restart_policy:
        condition: on-failure
        delay: 10s

  spark-worker:
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
        source: C:\path\to\your\project\spark
        target: /opt/spark/work-dir
    deploy:
      mode: global      # Spark Worker على كل جهاز
      restart_policy:
        condition: on-failure
        delay: 10s

  # ════════════════════════════════════════════════════════
  # PRODUCER & CONSUMER — على PC1 فقط
  # ════════════════════════════════════════════════════════

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
        source: C:\path\to\your\project\kafka
        target: /app
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.nodeid == node1
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
        source: C:\path\to\your\project\kafka
        target: /app
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.nodeid == node1
      restart_policy:
        condition: on-failure
        delay: 10s

  # ════════════════════════════════════════════════════════
  # PGADMIN — على PC1
  # ════════════════════════════════════════════════════════

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
          - node.labels.nodeid == node1
      restart_policy:
        condition: on-failure
        delay: 10s

  # ════════════════════════════════════════════════════════
  # AIRFLOW — على PC1
  # ════════════════════════════════════════════════════════

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
          - node.labels.nodeid == node1
      restart_policy:
        condition: on-failure
        delay: 10s

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
          - node.labels.nodeid == node1
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
        source: C:\path\to\your\project\dags
        target: /opt/airflow/dags
      - airflow-logs:/opt/airflow/logs
      - airflow-plugins:/opt/airflow/plugins
    command: ["airflow", "webserver"]
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.nodeid == node1
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
        source: C:\path\to\your\project\dags
        target: /opt/airflow/dags
      - airflow-logs:/opt/airflow/logs
      - airflow-plugins:/opt/airflow/plugins
    command: ["airflow", "scheduler"]
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.labels.nodeid == node1
      restart_policy:
        condition: on-failure
        delay: 15s

# ════════════════════════════════════════════════════════
# NETWORKS
# ════════════════════════════════════════════════════════

networks:
  stocks-net:
    external: true

# ════════════════════════════════════════════════════════
# VOLUMES — تشير لـ Shared Storage على PC1 عبر SMB
# ════════════════════════════════════════════════════════
# ملاحظة: هذه الـ Volumes يجب أن تكون مُنشأة مسبقاً على كل جهاز
# (راجع مرحلة 4 الخطوة 6.5)

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

## 9. مرحلة 7 — Deploy

### الخطوة 9.1 — تحضير ملف .env على PC1

في مجلد المشروع على PC1، أنشئ أو عدّل ملف `.env`:

```env
# Kaggle
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key

# Kafka
KAFKA_BOOTSTRAP=localhost:9092,localhost:9093,localhost:9094
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
AIRFLOW_SECRET_KEY=your-random-secret-key-here
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
```

لتوليد AIRFLOW_SECRET_KEY:

```powershell
python -c "import secrets; print(secrets.token_hex(32))"
```

### الخطوة 9.2 — عدّل المسارات في docker-stack.yml

افتح `docker-stack.yml` وابحث عن كل `C:\path\to\your\project` واستبدلها بالمسار الحقيقي، مثلاً:

```yaml
source: C:\Users\Mohamed\Desktop\stocks-pipeline\spark
```

وأيضاً استبدل كل `100.x.x.1`، `100.x.x.2`، `100.x.x.3` بالـ IPs الحقيقية.

وكل `PC1`، `PC2`، `PC3` بالـ Hostnames الحقيقية من أمر `hostname`.

### الخطوة 9.3 — Deploy الـ Stack

```powershell
# على PC1 — في مجلد المشروع
cd C:\path\to\your\project

docker stack deploy `
    --compose-file docker-stack.yml `
    --with-registry-auth `
    stocks
```

### الخطوة 9.4 — تابع الـ Deploy

```powershell
# شوف حالة كل الـ Services
docker stack services stocks

# شوف على أنهي جهاز نزلت كل service
docker stack ps stocks

# لو عايز تتابع بشكل مستمر
while ($true) {
    Clear-Host
    docker stack services stocks
    Start-Sleep -Seconds 5
}
```

### الخطوة 9.5 — انتظر حتى يكتمل كل شيء

قد يأخذ هذا 3-5 دقائق. تحقق من كل service إنها `1/1` في عمود `REPLICAS`.

---

## 10. مرحلة 8 — اختبار الـ Failover

### اختبار 1 — وقوع PC1

```powershell
# من PC2 — قبل الاختبار، تحقق من Leader
docker node ls

# أوقف Docker Desktop على PC1 (أو افصل الـ Network)
# أو من PC1 نفسه:
# docker swarm leave --force

# من PC2 بعد 30 ثانية
docker node ls
# هتشوف PC2 أو PC3 أصبح Leader

# تحقق من أن Kafka لا تزال تعمل
docker exec -it `
    $(docker ps -q -f "name=stocks_kafka-2") `
    kafka-topics --bootstrap-server kafka-2:29093 --list
```

### اختبار 2 — وقوع Kafka Broker

Kafka RF=3 مع min.insync.replicas=2 يعني إن الـ cluster يكمل حتى لو وقع broker واحد:

```powershell
# من أي جهاز
# أوقف container kafka-1 (مثلاً)
docker service scale stocks_kafka-1=0

# انتظر 30 ثانية
Start-Sleep -Seconds 30

# تحقق إن الـ Topic لا تزال تعمل عبر kafka-2 وkafka-3
docker exec -it `
    $(docker ps -q -f "name=stocks_kafka-2") `
    kafka-topics `
    --bootstrap-server kafka-2:29093,kafka-3:29094 `
    --describe --topic us-stocks-raw

# أعد تشغيل kafka-1
docker service scale stocks_kafka-1=1
```

### اختبار 3 — وقوع PostgreSQL والاسترداد

```powershell
# أوقف postgres
docker service scale stocks_postgres=0

# انتظر 10 ثواني ثم أعد تشغيله
Start-Sleep -Seconds 10
docker service scale stocks_postgres=1

# تحقق إن البيانات لم تُفقد
docker exec -it `
    $(docker ps -q -f "name=stocks_postgres") `
    psql -U stocks -d stocks_analytics -c "SELECT COUNT(*) FROM stock_summary;"
```

---

## 11. الـ URLs والـ Ports بعد الـ Deploy

| Service | URL (من أي جهاز) | Login |
|---|---|---|
| Kafka UI | http://100.x.x.1:8080 أو http://100.x.x.2:8080 أو http://100.x.x.3:8080 | لا يوجد |
| Spark Master UI | http://100.x.x.1:8081 | لا يوجد |
| MinIO Console | http://100.x.x.1:9001 | minioadmin / minioadmin |
| MinIO S3 API | http://100.x.x.1:9000 | — |
| pgAdmin | http://100.x.x.1:5050 | admin@stocks.com / admin123 |
| Airflow UI | http://100.x.x.1:8082 | admin / admin |
| PostgreSQL | 100.x.x.1:5432 | stocks / stocks123 |
| Kafka Broker 1 | 100.x.x.1:9092 | — |
| Kafka Broker 2 | 100.x.x.2:9093 | — |
| Kafka Broker 3 | 100.x.x.3:9094 | — |

### أوامر Spark Submit بعد الـ Deploy

```powershell
# Streaming Job
docker exec -it `
    $(docker ps -q -f "name=stocks_spark") `
    /opt/spark/bin/spark-submit `
    --master spark://spark:7077 `
    --conf spark.jars.ivy=/tmp/.ivy2 `
    /opt/spark/work-dir/streaming_job.py

# Analytics Job
docker exec -it `
    $(docker ps -q -f "name=stocks_spark") `
    /opt/spark/bin/spark-submit `
    --master local[4] `
    --conf spark.jars.ivy=/tmp/.ivy2 `
    --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" `
    --conf spark.hadoop.fs.s3a.path.style.access=true `
    --conf spark.hadoop.fs.s3a.access.key=minioadmin `
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin `
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false `
    /opt/spark/work-dir/analytics_job.py
```

---

## 12. Troubleshooting

### المشكلة 1 — `docker swarm join` فاشل

```powershell
# تحقق إن الـ Port 2377 مفتوح
Test-NetConnection -ComputerName 100.x.x.1 -Port 2377

# لو مسدود، تحقق من الـ Firewall
netsh advfirewall firewall show rule name="Docker Swarm Port 2377"

# لو مش موجود، أضفه مجدداً
netsh advfirewall firewall add rule name="Docker Swarm Port 2377" dir=in action=allow protocol=TCP localport=2377 remoteip=100.0.0.0/8
```

### المشكلة 2 — Service عالق في `Pending`

```powershell
# شوف السبب التفصيلي
docker service ps stocks_kafka-1 --no-trunc

# الأسباب الشائعة:
# أ) الـ Image مش موجودة
docker images | Select-String "stocks"

# ب) مشكلة في الـ Volume
docker volume ls
docker volume inspect minio-data

# ج) الـ Label غلط
docker node inspect PC1 --format "{{ .Spec.Labels }}"
# لو فيه مشكلة:
docker node update --label-add kafka.broker=1 PC1
```

### المشكلة 3 — Kafka Brokers مش بتتواصل

```powershell
# تحقق إن الـ Overlay Network شغّال
docker network inspect stocks-net

# تحقق من DNS داخل الـ Network
docker run --rm --network stocks-net `
    busybox nslookup kafka-1

docker run --rm --network stocks-net `
    busybox nslookup kafka-2
```

### المشكلة 4 — SMB Volume بيفشل

```powershell
# تحقق من الـ Share على PC1
net share stocks-data

# تحقق من الوصول
net use \\100.x.x.1\stocks-data /user:PC1_USERNAME PC1_PASSWORD

# لو في مشكلة credentials:
# روح لـ Control Panel → Credential Manager → Windows Credentials
# أضف: \\100.x.x.1 مع الـ Username وPassword
```

### المشكلة 5 — Docker Desktop لا يبدأ على Windows

```powershell
# تحقق من WSL2
wsl --list --verbose

# لو Ubuntu مش running
wsl --set-version Ubuntu-22.04 2
wsl -d Ubuntu-22.04

# أعد تشغيل Docker Desktop service
Restart-Service -Name "com.docker.service" -Force
```

### المشكلة 6 — Tailscale IP يتغير

Tailscale IPs ثابتة ولا تتغير ما دمت في نفس الـ Tailnet. لكن لو حذفت الجهاز وأضفته مجدداً، قد تحصل على IP مختلف. الحل: استخدم **Tailscale MagicDNS** بدل الـ IPs:

```
# بدل 100.x.x.1 استخدم اسم الجهاز:
PC1.tail12345.ts.net
PC2.tail12345.ts.net
PC3.tail12345.ts.net
```

اكتشف الـ DNS name:

```powershell
& "C:\Program Files\Tailscale\tailscale.exe" status
# هتلاقي الـ DNS names في الأعمدة
```

### المشكلة 7 — Spark Worker لا يتصل بـ Spark Master

```powershell
# تحقق إن الـ spark service موجود في الـ DNS
docker run --rm --network stocks-net `
    busybox nslookup spark

# تحقق من الـ Port
docker run --rm --network stocks-net `
    busybox nc -zv spark 7077
```

### أوامر المراقبة اليومية

```powershell
# حالة كل الـ Nodes
docker node ls

# حالة كل الـ Services
docker stack services stocks

# حالة كل الـ Containers وعلى أنهي جهاز
docker stack ps stocks

# Logs أي service
docker service logs stocks_kafka-1 -f --tail 50

# إيقاف كل شيء
docker stack rm stocks

# Scale service معين
docker service scale stocks_spark-worker=2

# Update image
docker service update --image stocks-spark:3.5.7-new stocks_spark
```

---

## 📝 ملاحظات ختامية

### ما يحدث عند وقوع كل جهاز

| الجهاز الذي يقع | التأثير | الاسترداد |
|---|---|---|
| PC1 | kafka-1 يتوقف، postgres يتوقف، minio يتوقف، spark master يتوقف | Kafka يكمل مع broker 2+3، Swarm يعيّن Leader جديد. تحتاج تبدأ postgres وminio يدوياً على PC2 |
| PC2 | kafka-2 يتوقف | Kafka يكمل مع broker 1+3 (RF=3، min.insync=2) |
| PC3 | kafka-3 يتوقف | Kafka يكمل مع broker 1+2 |
| أي جهازين في نفس الوقت | الـ Cluster يتجمّد (لا يقبل تغييرات) | أعد تشغيل أحد الجهازين |

### نصائح مهمة

1. **لا تُطفئ أكثر من جهاز واحد في نفس الوقت** — الـ Quorum يحتاج أغلبية.
2. **احفظ الـ Join Token** في مكان آمن — ستحتاجه لو أضفت جهازاً جديداً.
3. **تأكد من أن Tailscale شغّال دائماً** — هو شريان الحياة للـ Cluster.
4. **البيانات محفوظة على PC1** — في `C:\stocks-data`. خذ backup منها بانتظام.
5. **إذا انتقل الـ Leader** من PC1 لجهاز آخر، ستلاحظ إن MinIO وPostgres مش شغّالين لأنهم متقيدين بـ PC1. الحل المؤقت: شغّل docker service update لتغيير الـ Constraint.

---

*تاريخ الإنشاء: September 2026 | Real-Time US Stocks Data Pipeline — HA Cluster Guide*
