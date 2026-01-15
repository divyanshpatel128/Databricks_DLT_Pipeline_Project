# 🚀 End-to-End Databricks Delta Live Tables (DLT) Project

This project demonstrates a **complete end-to-end Data Engineering pipeline** built using **Databricks Delta Live Tables (DLT)** following the **Medallion Architecture (Bronze → Silver → Gold)**. The project covers data ingestion, transformation, optimization, and business-level reporting using curated views and dashboards.

---

## 📌 Project Overview

* Built an automated ETL pipeline using **Delta Live Tables (DLT)**
* Implemented **Medallion Architecture** for scalable and reliable data processing
* Separated **ETL logic** and **business analytics** for better maintainability
* Designed **business-ready views and dashboards** on top of Gold tables
* Suitable for real-world analytics and reporting use cases

---

## 🏗️ Project Structure

```
Main_DLT_Project/
│
├── https://github.com/divyanshpatel128/Databricks_DLT_Pipeline_Project/tree/main/ETL_Medallion_Architecture
│   ├── https://github.com/divyanshpatel128/Databricks_DLT_Pipeline_Project/tree/main/ETL_Medallion_Architecture/Bronze        # Raw data ingestion (DLT)
│   ├── https://github.com/divyanshpatel128/Databricks_DLT_Pipeline_Project/tree/main/ETL_Medallion_Architecture/Silver     # Cleaned & transformed data (DLT)
│   ├── https://github.com/divyanshpatel128/Databricks_DLT_Pipeline_Project/tree/main/ETL_Medallion_Architecture/Gold         # SCD-2 & business-ready data (DLT)
│   
│
├── https://github.com/divyanshpatel128/Databricks_DLT_Pipeline_Project/tree/main/Business_Dashboard_Views
│   ├── https://github.com/divyanshpatel128/Databricks_DLT_Pipeline_Project/tree/main/Business_Dashboard_Views/Sql_View     # Business SQL views
│   ├── dashboards/   # Databricks SQL / BI dashboards
│   
```

---

## 🥉 Bronze Layer – Raw Data

**Purpose:**

* Ingest raw data from source systems into Databricks

**Key Characteristics:**

* Minimal or no transformations
* Data stored in original format
* May contain duplicates, nulls, or inconsistent values
* Stored as **Delta tables** using DLT

**Use Case Examples:**

* Raw transaction data
* Event or log data
* Streaming or batch ingestion using Auto Loader

---

## 🥈 Silver Layer – Cleaned & Transformed Data

**Purpose:**

* Clean, standardize, and enrich data from the Bronze layer

**Key Characteristics:**

* Data cleansing (null handling, deduplication)
* Data type corrections and standardization
* Business rule application
* Joins between related datasets
* Stored as **Delta tables** using DLT

**Use Case Examples:**

* Cleaned customer and transaction data
* Enriched datasets ready for analytics

---

## 🥇 Gold Layer – Business-Ready Data

**Purpose:**

* Provide analytics-optimized, business-focused datasets

**Key Characteristics:**

* Aggregated and summarized metrics
* Optimized for fast querying
* Designed for dashboards and reporting
* Can be used directly by BI tools

**Use Case Examples:**

* KPI metrics
* Daily/Monthly performance reports
* Customer and product analytics

---

## 📊 Business Dashboard & Views

**Purpose:**

* Deliver interactive, business-ready insights for stakeholders

**Description:**

* Dashboards and SQL views built on top of Gold tables
* Visualizes KPIs, trends, and performance metrics
* Enables data-driven decision-making using reliable curated data

---

## ⚙️ Technologies Used

* **Databricks**
* **Delta Live Tables (DLT)**
* **Delta Lake**
* **Apache Spark**
* **SQL & PySpark**
* **Databricks SQL Dashboards**

---

## ✅ Key Highlights

* End-to-end automated data pipeline
* Production-ready medallion architecture
* Clear separation of ETL and business logic
* Scalable and maintainable project structure
* Ideal for Data Engineer / Analytics Engineer portfolios

---

## 📌 Author

**Divyansh Patel**
Data Engineer | SQL | Databricks | Analytics

🔗 LinkedIn: https://www.linkedin.com/in/divyansh-patel-dataanalyst/

---

⭐ If you find this project useful, feel free to star the repository!
