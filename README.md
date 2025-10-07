# 🏭 OEE Edge ETL Project

### 📘 Overview
This project implements an **Edge-level OEE (Overall Equipment Effectiveness)** data pipeline.  
It aggregates raw production signals from **ThingsBoard Edge (Cassandra)**, combines them with dimensional data from **PostgreSQL**, and writes computed KPIs into fact tables every minute.

The results are then pushed to **a virtual OEE device** on ThingsBoard Edge to be synchronized to the **ThingsBoard Platform** automatically.

---

### ⚙️ System Architecture
    ┌────────────┐
    │ OPC UA PLC │
    └─────┬──────┘
          │
          ▼
┌───────────────────────┐
│ ThingsBoard Edge      │
│ - OPC driver (Gateway)│
│ - Store: Cassandra    │
└─────────┬─────────────┘
          │
          ▼
┌───────────────────────┐
│ Python ETL Jobs       │
│ - Read raw data       │
│ - Join dim (PostgreSQL)│
│ - Compute OEE/min, state│
│ - Upsert fact tables  │
└─────────┬─────────────┘
          │
          ▼
┌───────────────────────┐
│ PostgreSQL (OEE DW)   │
│ - dim_line, dim_reason│
│ - fact_production_min │
│ - fact_state_event    │
└─────────┬─────────────┘
          │
          ▼
┌───────────────────────┐
│ ThingsBoard Platform  │
│ - OEE dashboard       │
│ - Alarms, charts      │
└───────────────────────┘

---

### 🗂️ Project Structure
oee-edge/
├── src/
│ ├── etl_minute.py # Main ETL job (every 60s)
│ ├── io_pg.py # PostgreSQL read/write helpers
│ ├── io_cas.py # Cassandra read helpers
│ ├── utils.py # Time utils, conversions, backfill logic
│
├── configs/ # Optional: store YAML/JSON configs
├── logs/ # ETL runtime logs (gitignored)
├── .env # Local env vars (gitignored)
├── .gitignore # Ignore venv, logs, .env, etc.
├── requirements.txt # Python dependencies
└── README.md # This file

---

### 🧰 Environment Setup (Ubuntu)
```bash
# 1. Clone repo
git clone https://github.com/trantuyen1991/oee_edge.git
cd oee_edge

# 2. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create .env file
cp .env.example .env
# (Edit connection info for PostgreSQL, Cassandra, timezone, etc.)

#🕒 Running the ETL job manually
source .venv/bin/activate
python src/etl_minute.py

#To enable auto scheduling via systemd:
sudo cp deploy/oee-etl.service /etc/systemd/system/
sudo cp deploy/oee-etl.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now oee-etl.timer

#🧩 Core Features
# - Watermark + backfill to ensure late data handled correctly
# - Idempotent UPSERT for PostgreSQL
# - UTC-aligned timestamps
# - Line-based configuration (multi-line ready)
# - Ready for ThingsBoard integration

#📜 License
# - This project is open-sourced for educational and industrial reference.
# - Author: @trantuyen1991