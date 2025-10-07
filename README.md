# 🏭 OEE Edge ETL Project

### 📘 Overview
This project implements an **Edge-level OEE (Overall Equipment Effectiveness)** data pipeline.  
It aggregates raw production signals from **ThingsBoard Edge (Cassandra)**, combines them with dimensional data from **PostgreSQL**, and writes computed KPIs into fact tables every minute.

The results are then pushed to **a virtual OEE device** on ThingsBoard Edge to be synchronized to the **ThingsBoard Platform** automatically.

---

### ⚙️ System Architecture
