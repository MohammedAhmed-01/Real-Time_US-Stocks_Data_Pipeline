# 🔗 Tailscale — Connect 3 Devices for Docker Swarm Cluster
## Real-Time US Stocks Data Pipeline

> **Goal:** Create a secure private network between 3 Windows machines (1 Master + 2 Workers) using Tailscale, so Docker Swarm can communicate across them as if they were on the same LAN.

---

## 📋 Table of Contents

1. [Before You Start — Record Your Info](#1-before-you-start--record-your-info)
2. [What is Tailscale and Why We Use It](#2-what-is-tailscale-and-why-we-use-it)
3. [Create a Tailscale Account](#3-create-a-tailscale-account)
4. [Install Tailscale on All 3 Devices](#4-install-tailscale-on-all-3-devices)
5. [Log In on All 3 Devices (Same Account)](#5-log-in-on-all-3-devices-same-account)
6. [Verify All 3 Devices Are Connected](#6-verify-all-3-devices-are-connected)
7. [Find Each Device's Tailscale IP](#7-find-each-devices-tailscale-ip)
8. [Test Connectivity Between Devices](#8-test-connectivity-between-devices)
9. [Open Required Firewall Ports](#9-open-required-firewall-ports)
10. [Verify Ports Are Open](#10-verify-ports-are-open)
11. [Tailscale Admin Console — Key Settings](#11-tailscale-admin-console--key-settings)
12. [Troubleshooting](#12-troubleshooting)
13. [Quick Reference Card](#13-quick-reference-card)

---

## 1. Before You Start — Record Your Info

Open **PowerShell** on each machine and run `hostname`. Write down the results here — you will need them throughout this guide.

```
┌─────────────────────────────────────────────────────────────┐
│                  FILL THIS IN FIRST                         │
├────────────┬────────────────────────┬───────────────────────┤
│ Role       │ Hostname (hostname cmd)│ Tailscale IP (step 7) │
├────────────┼────────────────────────┼───────────────────────┤
│ MASTER     │ ______________________ │ 100.___.___.___ ← M   │
│ WORKER 1   │ ______________________ │ 100.___.___.___ ← W1  │
│ WORKER 2   │ ______________________ │ 100.___.___.___ ← W2  │
└────────────┴────────────────────────┴───────────────────────┘
```

To get your hostname, open PowerShell and run:

```powershell
hostname
```

> ⚠️ **Assign roles now.** The Master should be your most powerful machine — it runs Kafka broker 1, MinIO, PostgreSQL, Spark Master, and Airflow. Workers only run Kafka brokers 2 & 3 and Spark Workers.

---

## 2. What is Tailscale and Why We Use It

Tailscale creates a **secure mesh VPN** between your devices. Instead of complicated port forwarding, static IPs, or being on the same physical network, every device gets a stable private IP in the `100.x.x.x` range.

**Why Tailscale for this project:**

| Problem Without Tailscale | How Tailscale Solves It |
|---|---|
| Devices on different networks/ISPs | All devices appear on one private network |
| Dynamic home IP addresses | Tailscale IPs are stable and never change |
| Complex firewall rules | Tailscale handles NAT traversal automatically |
| Docker Swarm needs fixed IPs | Each device gets a permanent `100.x.x.x` IP |
| No IT department / no static IP | Works on any home or office internet |

**How it works:**

```
Your 3 PCs (anywhere in the world)
        │
        ▼
  ┌─────────────┐
  │  Tailscale  │  ← Coordination server (knows where everyone is)
  │   Network   │
  └──────┬──────┘
         │
    Encrypted peer-to-peer tunnel (WireGuard)
         │
  ┌──────┴───────────────────────┐
  │  Your Private Tailnet        │
  │                              │
  │  Master  → 100.x.x.1        │
  │  Worker1 → 100.x.x.2        │
  │  Worker2 → 100.x.x.3        │
  └──────────────────────────────┘
```

---

## 3. Create a Tailscale Account

> ⚠️ **You only need ONE account. All 3 machines log in with the same account.**

1. Open your browser and go to **https://tailscale.com**
2. Click **Get started** (top right)
3. Choose a sign-in method — pick one you have on all 3 machines:
   - ✅ **Google** (recommended — most people have this)
   - ✅ **Microsoft** (if you use Microsoft accounts)
   - ✅ **GitHub**
4. Complete the sign-up process
5. You will land on the **Tailscale Admin Console** at `https://login.tailscale.com/admin`

> 📝 **Write down your login method:** ________________________
> You must use the exact same method on all 3 machines.

---

## 4. Install Tailscale on All 3 Devices

> ⚠️ **Do this on ALL 3 machines — Master, Worker 1, and Worker 2.**

### Step 4.1 — Download Tailscale

On each machine:

1. Open your browser and go to: **https://tailscale.com/download/windows**
2. Click **Download for Windows**
3. Save the installer file (it will be named something like `tailscale-setup-x.xx.x.exe`)

### Step 4.2 — Install Tailscale

1. Double-click the downloaded installer
2. If Windows asks **"Do you want to allow this app to make changes to your device?"** — click **Yes**
3. The installer runs automatically (no configuration needed during install)
4. When done, you will see the Tailscale icon appear in your **System Tray** (the area near the clock, bottom-right of your screen)

   The icon looks like this: 🔵 (a blue circle or arrow icon)

5. If you don't see it, click the **▲ arrow** in the system tray to show hidden icons

### Step 4.3 — Verify the Installation

Open PowerShell on each machine and run:

```powershell
& "C:\Program Files\Tailscale\tailscale.exe" version
```

Expected output (version numbers may differ):

```
1.xx.x
  tailscale commit: abc123...
  go version: go1.xx.x
```

If you see a version number, the installation succeeded.

---

## 5. Log In on All 3 Devices (Same Account)

> ⚠️ **Critical: Use the EXACT SAME Google/Microsoft/GitHub account on all 3 machines.**

Do the following on each machine — **one machine at a time**:

### Step 5.1 — Log In via System Tray (Easiest Method)

1. Find the Tailscale icon in the System Tray (bottom-right, near the clock)
2. **Right-click** the Tailscale icon
3. Click **Log in**
4. Your default browser will open to the Tailscale login page
5. Sign in with your account (Google / Microsoft / GitHub)
6. After signing in, the browser will show: **"Device connected successfully"** or a similar confirmation
7. Close the browser — Tailscale is now running on this machine

### Step 5.2 — Alternative: Log In via PowerShell

If the system tray method doesn't work:

```powershell
# Open PowerShell as Administrator
# This will open a browser window for login
& "C:\Program Files\Tailscale\tailscale.exe" up
```

### Step 5.3 — Confirm Login Was Successful

After logging in, right-click the Tailscale icon in the System Tray. You should see:

```
✓ Connected
This device: [your-machine-name]
IP: 100.x.x.x
```

If it says **"Connected"** — this machine is ready. Repeat steps 5.1 to 5.3 on the next machine.

---

## 6. Verify All 3 Devices Are Connected

Once all 3 machines are logged in, verify them in the **Tailscale Admin Console**:

1. On any machine, open your browser and go to: **https://login.tailscale.com/admin/machines**
2. Log in with your account if prompted
3. You should see a table listing **all 3 machines** like this:

```
┌─────────────────────┬──────────────┬────────────┬────────┐
│ Machine Name        │ IP Address   │ OS         │ Status │
├─────────────────────┼──────────────┼────────────┼────────┤
│ MASTER-PC           │ 100.x.x.1    │ Windows    │ ✓      │
│ WORKER1-PC          │ 100.x.x.2    │ Windows    │ ✓      │
│ WORKER2-PC          │ 100.x.x.3    │ Windows    │ ✓      │
└─────────────────────┴──────────────┴────────────┴────────┘
```

> ✅ All 3 machines must show as **Connected** (green dot or checkmark) before continuing.

If a machine is missing from the list:
- Make sure Tailscale is running on that machine (check System Tray)
- Make sure you logged in with the **same account** on that machine
- Try running `tailscale up` in PowerShell on the missing machine

---

## 7. Find Each Device's Tailscale IP

> You need these IPs for Docker Swarm configuration. Write them in the table from Step 1.

### Method A — From the System Tray (Easiest)

1. Right-click the Tailscale icon in the System Tray
2. Your Tailscale IP is shown directly in the menu (e.g., `100.64.0.1`)

### Method B — From PowerShell

Run this on each machine:

```powershell
& "C:\Program Files\Tailscale\tailscale.exe" ip -4
```

Example output:

```
100.64.0.1
```

### Method C — From Network Settings

```powershell
Get-NetIPAddress -AddressFamily IPv4 |
    Where-Object { $_.IPAddress -like "100.*" } |
    Select-Object IPAddress, InterfaceAlias
```

Example output:

```
IPAddress    InterfaceAlias
---------    --------------
100.64.0.1   Tailscale
```

### Method D — From the Admin Console

1. Go to **https://login.tailscale.com/admin/machines**
2. The IP for each machine is listed in the **Addresses** column

---

## 8. Test Connectivity Between Devices

> This is the most important verification step. All ping tests must succeed.

### Step 8.1 — Ping from Master to Both Workers

Open **PowerShell on the Master** and run (replace with your actual IPs):

```powershell
# Replace with Worker 1's actual Tailscale IP
ping 100.x.x.2 -n 4

# Replace with Worker 2's actual Tailscale IP
ping 100.x.x.3 -n 4
```

Expected output for each ping:

```
Pinging 100.x.x.2 with 32 bytes of data:
Reply from 100.x.x.2: bytes=32 time=8ms TTL=64
Reply from 100.x.x.2: bytes=32 time=6ms TTL=64
Reply from 100.x.x.2: bytes=32 time=7ms TTL=64
Reply from 100.x.x.2: bytes=32 time=6ms TTL=64

Ping statistics for 100.x.x.2:
    Packets: Sent = 4, Received = 4, Lost = 0 (0% loss)
```

> ✅ You need **0% packet loss** (Lost = 0). If you see packet loss, see the Troubleshooting section.

### Step 8.2 — Ping from Workers to Master

Open **PowerShell on Worker 1** and run:

```powershell
# Replace with Master's actual Tailscale IP
ping 100.x.x.1 -n 4
```

Open **PowerShell on Worker 2** and run:

```powershell
# Replace with Master's actual Tailscale IP
ping 100.x.x.1 -n 4
```

Both must show 0% packet loss.

### Step 8.3 — Ping Workers to Each Other

Open **PowerShell on Worker 1** and run:

```powershell
# Replace with Worker 2's actual Tailscale IP
ping 100.x.x.3 -n 4
```

### Step 8.4 — Test with Tailscale's Built-in Ping

Tailscale has its own ping command that shows more detail:

```powershell
# Run on Master — replace with Worker 1's IP
& "C:\Program Files\Tailscale\tailscale.exe" ping 100.x.x.2
```

Expected output:

```
pong from WORKER1-PC (100.x.x.2) via DERP(nyc) in 45ms
pong from WORKER1-PC (100.x.x.2) via DERP(nyc) in 12ms
pong from WORKER1-PC (100.x.x.2) via 192.168.x.x:41641 in 3ms
```

The last line showing a direct connection (`via 192.168.x.x`) means the connection is peer-to-peer (fastest). If it only shows `via DERP` (relay), it's still working but slightly slower.

---

## 9. Open Required Firewall Ports

Docker Swarm requires specific ports to be open between all machines. Open these on **all 3 machines**.

> ⚠️ Open **PowerShell as Administrator** for these commands (right-click PowerShell → Run as Administrator).

### Port 2377 — Docker Swarm Management (TCP)

```powershell
netsh advfirewall firewall add rule `
    name="Docker-Swarm-2377-TCP" `
    dir=in `
    action=allow `
    protocol=TCP `
    localport=2377 `
    remoteip=100.0.0.0/8 `
    description="Docker Swarm management - Tailscale only"
```

### Port 7946 — Container Network Discovery (TCP + UDP)

```powershell
netsh advfirewall firewall add rule `
    name="Docker-Swarm-7946-TCP" `
    dir=in `
    action=allow `
    protocol=TCP `
    localport=7946 `
    remoteip=100.0.0.0/8 `
    description="Docker Swarm node comm TCP - Tailscale only"

netsh advfirewall firewall add rule `
    name="Docker-Swarm-7946-UDP" `
    dir=in `
    action=allow `
    protocol=UDP `
    localport=7946 `
    remoteip=100.0.0.0/8 `
    description="Docker Swarm node comm UDP - Tailscale only"
```

### Port 4789 — Overlay Network Traffic (UDP)

```powershell
netsh advfirewall firewall add rule `
    name="Docker-Swarm-4789-UDP" `
    dir=in `
    action=allow `
    protocol=UDP `
    localport=4789 `
    remoteip=100.0.0.0/8 `
    description="Docker Swarm overlay network - Tailscale only"
```

### Application Ports — Stocks Pipeline (Master only)

Open these **only on the Master** so you can access services from any of the 3 machines:

```powershell
# Kafka brokers
netsh advfirewall firewall add rule name="Kafka-9092" dir=in action=allow protocol=TCP localport=9092 remoteip=100.0.0.0/8
netsh advfirewall firewall add rule name="Kafka-9093" dir=in action=allow protocol=TCP localport=9093 remoteip=100.0.0.0/8
netsh advfirewall firewall add rule name="Kafka-9094" dir=in action=allow protocol=TCP localport=9094 remoteip=100.0.0.0/8

# Kafka UI
netsh advfirewall firewall add rule name="KafkaUI-8080" dir=in action=allow protocol=TCP localport=8080 remoteip=100.0.0.0/8

# Spark Master UI
netsh advfirewall firewall add rule name="SparkUI-8081" dir=in action=allow protocol=TCP localport=8081 remoteip=100.0.0.0/8

# Spark submit port
netsh advfirewall firewall add rule name="Spark-7077" dir=in action=allow protocol=TCP localport=7077 remoteip=100.0.0.0/8

# MinIO API and Console
netsh advfirewall firewall add rule name="MinIO-9000" dir=in action=allow protocol=TCP localport=9000 remoteip=100.0.0.0/8
netsh advfirewall firewall add rule name="MinIO-9001" dir=in action=allow protocol=TCP localport=9001 remoteip=100.0.0.0/8

# PostgreSQL
netsh advfirewall firewall add rule name="Postgres-5432" dir=in action=allow protocol=TCP localport=5432 remoteip=100.0.0.0/8

# pgAdmin
netsh advfirewall firewall add rule name="pgAdmin-5050" dir=in action=allow protocol=TCP localport=5050 remoteip=100.0.0.0/8

# Airflow
netsh advfirewall firewall add rule name="Airflow-8082" dir=in action=allow protocol=TCP localport=8082 remoteip=100.0.0.0/8

# SMB for shared volumes
netsh advfirewall firewall add rule name="SMB-445-Tailscale" dir=in action=allow protocol=TCP localport=445 remoteip=100.0.0.0/8
```

---

## 10. Verify Ports Are Open

### Check that the firewall rules were created

```powershell
# List all Swarm-related rules
netsh advfirewall firewall show rule name="Docker-Swarm-2377-TCP"
netsh advfirewall firewall show rule name="Docker-Swarm-7946-TCP"
netsh advfirewall firewall show rule name="Docker-Swarm-7946-UDP"
netsh advfirewall firewall show rule name="Docker-Swarm-4789-UDP"
```

Each should show:

```
Rule Name:                            Docker-Swarm-2377-TCP
----------------------------------------------------------------------
Enabled:                              Yes
Direction:                            In
Profiles:                             Domain,Private,Public
Grouping:
LocalIP:                              Any
RemoteIP:                             100.0.0.0/8
Protocol:                             TCP
LocalPort:                            2377
RemotePort:                           Any
Edge traversal:                       No
Action:                               Allow
```

### Test port connectivity from one machine to another

Run on **Worker 1** to test Master's port 2377:

```powershell
# Replace 100.x.x.1 with Master's actual Tailscale IP
Test-NetConnection -ComputerName 100.x.x.1 -Port 2377
```

Expected output:

```
ComputerName     : 100.x.x.1
RemoteAddress    : 100.x.x.1
RemotePort       : 2377
InterfaceAlias   : Tailscale
SourceAddress    : 100.x.x.2
TcpTestSucceeded : True
```

> ✅ `TcpTestSucceeded : True` means the port is reachable.

Run the same test for other critical ports:

```powershell
# Test all Docker Swarm ports from Worker 1 to Master
Test-NetConnection -ComputerName 100.x.x.1 -Port 2377
Test-NetConnection -ComputerName 100.x.x.1 -Port 7946
Test-NetConnection -ComputerName 100.x.x.1 -Port 9092
```

---

## 11. Tailscale Admin Console — Key Settings

Visit **https://login.tailscale.com/admin** to manage your Tailnet.

### 11.1 — Machines Page

Go to **Machines** tab. You should see all 3 devices listed. Useful columns:

| Column | What it shows |
|---|---|
| Name | Device hostname |
| Addresses | Tailscale IP (the `100.x.x.x` you need) |
| Last Seen | When the device last connected |
| OS | Windows |
| Tags | Optional labels |

### 11.2 — Disable Key Expiry (Recommended)

By default, Tailscale authentication keys expire after 180 days, which means machines get disconnected and need to re-authenticate. For a server/cluster, disable this:

1. In the **Machines** tab, click the **three dots (⋯)** next to each machine
2. Click **Disable key expiry**
3. Confirm
4. Repeat for all 3 machines

You will see a green badge: **"Key expiry disabled"**

> ✅ Do this for all 3 machines so your cluster stays connected without manual re-authentication.

### 11.3 — Enable MagicDNS (Optional but Useful)

MagicDNS lets you reach machines by hostname instead of IP (e.g., `ping master-pc` instead of `ping 100.x.x.1`):

1. Go to **DNS** tab in the Admin Console
2. Under **MagicDNS**, click **Enable MagicDNS**
3. After enabling, you can use machine names directly in Docker Swarm commands

### 11.4 — Check Connection Type

In the **Machines** tab, click on any machine to see connection details. You want to see:

```
Direct connection via WireGuard
```

Not:

```
Relayed through DERP server
```

Direct connections are faster. If you see DERP relay, see the troubleshooting section.

---

## 12. Troubleshooting

### Problem: A machine doesn't appear in the Admin Console

**Symptoms:** Only 2 machines appear at `https://login.tailscale.com/admin/machines`

**Causes and fixes:**

```powershell
# 1. Check if Tailscale is running on the missing machine
Get-Process tailscale -ErrorAction SilentlyContinue

# 2. Check Tailscale service status
Get-Service -Name Tailscale -ErrorAction SilentlyContinue

# 3. Start Tailscale if it's not running
Start-Service Tailscale

# 4. Force re-authentication
& "C:\Program Files\Tailscale\tailscale.exe" logout
& "C:\Program Files\Tailscale\tailscale.exe" up
```

Also verify: did you use the **exact same Google/Microsoft/GitHub account**? Different accounts create different Tailnets and cannot see each other.

---

### Problem: Ping succeeds but with high latency (>100ms)

**Cause:** Tailscale is using a relay server (DERP) instead of a direct peer-to-peer connection.

**Fix — Check the connection type:**

```powershell
& "C:\Program Files\Tailscale\tailscale.exe" ping 100.x.x.2
```

If it shows `via DERP`, try:

```powershell
# Force Tailscale to try direct connection
& "C:\Program Files\Tailscale\tailscale.exe" netcheck
```

This runs a network check and may establish direct connections. Usually direct connections form automatically within a few minutes.

---

### Problem: Ping fails — Request timed out

**Step 1 — Check if Tailscale is connected on the target machine:**

```powershell
& "C:\Program Files\Tailscale\tailscale.exe" status
```

Expected output if connected:

```
100.x.x.1   master-pc   [your-account]@gmail.com  windows  active; relay "nyc"
100.x.x.2   worker1-pc  [your-account]@gmail.com  windows  active; relay "fra"
100.x.x.3   worker2-pc  [your-account]@gmail.com  windows  active; relay "syd"
```

**Step 2 — Check Windows Firewall isn't blocking ICMP (ping):**

```powershell
# Allow ping through Windows Firewall (run as Administrator)
netsh advfirewall firewall add rule `
    name="Allow-ICMP-Tailscale" `
    dir=in `
    action=allow `
    protocol=icmpv4 `
    remoteip=100.0.0.0/8
```

**Step 3 — Restart Tailscale:**

```powershell
Restart-Service Tailscale
Start-Sleep 5
& "C:\Program Files\Tailscale\tailscale.exe" status
```

---

### Problem: Port test fails — TcpTestSucceeded : False

**Step 1 — Verify the firewall rule exists:**

```powershell
netsh advfirewall firewall show rule name="Docker-Swarm-2377-TCP"
```

If no output — the rule doesn't exist. Re-run the commands from Step 9.

**Step 2 — Check if Docker is running on the target machine:**

```powershell
# The port won't be listening if Docker isn't running
docker info
```

**Step 3 — Temporarily disable Windows Firewall to test (then re-enable):**

```powershell
# Disable (TEST ONLY — re-enable after)
Set-NetFirewallProfile -Profile Domain,Public,Private -Enabled False

# Run your test
Test-NetConnection -ComputerName 100.x.x.1 -Port 2377

# IMPORTANT: Re-enable firewall
Set-NetFirewallProfile -Profile Domain,Public,Private -Enabled True
```

If the test passes with the firewall off, the issue is a missing or wrong firewall rule.

---

### Problem: Tailscale keeps disconnecting

**Fix — Prevent Windows from sleeping the network adapter:**

```powershell
# Open PowerShell as Administrator
# Find your network adapters
Get-NetAdapter | Select-Object Name, InterfaceDescription

# Disable power saving for Tailscale / main network adapter
# (Replace "Ethernet" with your adapter name)
$adapter = Get-NetAdapter -Name "Ethernet"
$adapter | Set-NetAdapterPowerManagement -WakeOnMagicPacket Disabled
```

Also in Windows settings:
1. Open **Settings** → **System** → **Power & sleep**
2. Set **Sleep** to **Never** (for a server/cluster machine)

---

### Problem: Two machines can ping each other but not the third

This usually means the third machine has a firewall issue or isn't on the same Tailnet.

```powershell
# On the machine that can't be reached — run as Administrator
# Check which profile the Tailscale adapter uses
Get-NetConnectionProfile | Where-Object { $_.InterfaceAlias -like "*Tailscale*" }
```

If it shows **Public** network profile, Windows applies stricter firewall rules. Fix it:

```powershell
# Find the Tailscale interface index
$tail = Get-NetAdapter | Where-Object { $_.InterfaceDescription -like "*Tailscale*" }

# Set it to Private (allows more traffic)
Set-NetConnectionProfile -InterfaceIndex $tail.ifIndex -NetworkCategory Private
```

---

## 13. Quick Reference Card

### Tailscale Commands (PowerShell)

```powershell
# Check status of all connected devices
& "C:\Program Files\Tailscale\tailscale.exe" status

# Get this machine's Tailscale IP
& "C:\Program Files\Tailscale\tailscale.exe" ip -4

# Ping another device through Tailscale
& "C:\Program Files\Tailscale\tailscale.exe" ping 100.x.x.2

# Run network diagnostics
& "C:\Program Files\Tailscale\tailscale.exe" netcheck

# Disconnect this machine from Tailnet
& "C:\Program Files\Tailscale\tailscale.exe" down

# Reconnect this machine to Tailnet
& "C:\Program Files\Tailscale\tailscale.exe" up

# Log out (will require re-authentication to reconnect)
& "C:\Program Files\Tailscale\tailscale.exe" logout
```

### Your Machine IPs (fill in from Step 7)

```
MASTER_IP  = 100.___.___.___ 
WORKER1_IP = 100.___.___.___ 
WORKER2_IP = 100.___.___.___ 
```

### Ports Required by Docker Swarm

| Port | Protocol | Purpose | Open On |
|---|---|---|---|
| `2377` | TCP | Swarm management | All 3 machines |
| `7946` | TCP + UDP | Node communication | All 3 machines |
| `4789` | UDP | Overlay network (VXLAN) | All 3 machines |
| `9092-9094` | TCP | Kafka brokers | Master |
| `7077` | TCP | Spark submit | Master |
| `8080-8081` | TCP | Kafka UI / Spark UI | Master |
| `9000-9001` | TCP | MinIO API / Console | Master |
| `5432` | TCP | PostgreSQL | Master |
| `5050` | TCP | pgAdmin | Master |
| `8082` | TCP | Airflow UI | Master |
| `445` | TCP | SMB shared volumes | Master |

### Checklist — Tailscale Setup Complete ✅

```
[ ] Tailscale account created
[ ] Tailscale installed on Master
[ ] Tailscale installed on Worker 1
[ ] Tailscale installed on Worker 2
[ ] Logged in with SAME account on all 3 machines
[ ] All 3 machines appear in Admin Console (Machines tab)
[ ] Key expiry disabled for all 3 machines
[ ] Tailscale IPs recorded for all 3 machines
[ ] Ping from Master → Worker 1: 0% packet loss
[ ] Ping from Master → Worker 2: 0% packet loss
[ ] Ping from Worker 1 → Master: 0% packet loss
[ ] Ping from Worker 2 → Master: 0% packet loss
[ ] Firewall ports opened on all 3 machines (2377, 7946, 4789)
[ ] Application ports opened on Master (9092, 7077, 9000, etc.)
[ ] Port connectivity tests pass (TcpTestSucceeded: True)
```

---

## What's Next

Once all items in the checklist above are ticked, your 3 machines are ready for Docker Swarm. The next steps are:

1. **Install WSL2 + Ubuntu** on all 3 machines
2. **Install Docker Desktop** on all 3 machines  
3. **Initialize Docker Swarm** — `docker swarm init --advertise-addr MASTER_IP` on the Master
4. **Join Workers to Swarm** — run the `docker swarm join` command on Worker 1 and Worker 2
5. **Deploy the stack** — `docker stack deploy -c docker-stack.yml stocks`

See `distributed_cluster_HA_windows.md` for the complete Docker Swarm setup guide.

---

*Guide version: September 2026 | Real-Time US Stocks Data Pipeline*
