# airbnb-elt-pipeline
End-to-end ELT pipeline using Airflow, dbt, and Postgres for Airbnb and Census data

# Airbnb & Census ELT Pipeline with Airflow and dbt

This project builds a production-ready ELT pipeline using Airflow and dbt Cloud to process Airbnb and Census data for Sydney. It uses a Medallion architecture and supports business insights via a data mart layer.

## 🔧 Tech Stack
- Apache Airflow (Cloud Composer)
- PostgreSQL (hosted on GCP)
- dbt Cloud
- Medallion Architecture (Bronze → Silver → Gold)
- SQL (Postgres dialect)

## 📁 Project Structure
- `airflow_dags/`: Orchestration logic (Airflow)
- `dbt/`: Models, snapshots, and configs
- `sql/part1/`: Raw ingestion queries
- `sql/part4/`: Ad-hoc business analyses
- `reports/`: Final report and diagrams

## 🧠 Key Business Insights
- Revenue by LGA and listing type
- Demographic comparison of top/low performing LGAs
- Host distribution across LGAs
- Revenue vs median mortgage repayments

## 📈 Outputs
- 3 Data Marts:
  - `dm_listing_neighbourhood`
  - `dm_property_type`
  - `dm_host_neighbourhood`

## 🖼️ Architecture Overview
![Architecture Diagram](assets/architecture.png)

---

Let me know if you’d like help generating a GitHub badge, uploading your .zip, or linking this to your portfolio site.
