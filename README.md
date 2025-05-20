
# Healthcare ETL on Databricks with Azure Integration

This project implements a modern healthcare data engineering pipeline using **Azure Data Factory**, **Azure Databricks**, and **Delta Lake**. It integrates data from SQL databases, API sources, and flat file uploads, transforming them into a unified analytics-ready format based on the **Medallion Architecture** and **Common Data Model (CDM)** standards.

## 🏥 Data Sources

- **SQL Server Database** hosting data for **2 hospitals**, each containing:
  - `Transactions`
  - `Doctors` (Providers)
  - `Patients`
  - `Departments`
  - `Encounters`
  - Used [Filess.io](https://dash.filess.io/) for SQL Data hosting

- **External APIs** for:
  - **ICD Codes**
  - **NPI Registry**

- **File-based Data:**
  - Claims data uploaded by the Claims Team (manually)
  - CPT Codes available in the Landing folder

### ERD
<img width="700" alt="SCR-20250520-lfug" src="https://github.com/user-attachments/assets/a24b6e24-7050-4947-a38c-a648ba9e70de" />



## 🏗 Architecture Overview

### 🔁 Architecture Pattern: **Medallion Architecture (Bronze → Silver → Gold)**

- **Bronze Layer:** Raw data ingestion from SQL DB, APIs, and file uploads
- **Silver Layer:** Cleaned and joined tables using business logic
- **Gold Layer:** Aggregated and analytics-ready tables for reporting and dashboards

## ⚙️ Key Features

### ✅ Data Engineering

- **SCD Type 2** implementation for managing historical changes in dimensions
- **Common Data Model (CDM)** adopted for standardizing healthcare data
- **Historical + Incremental Data Loads** using ADF and watermarking

### 🔐 Security

- **Azure Key Vault** used for secure credential management (SQL passwords, secrets)
- **Secret Scopes** created in Databricks to access Key Vault secrets
- **Secure Databricks ↔ ADLS Gen2** connectivity using **Microsoft Entra ID** Service Principal (formerly Azure AD)

### 🚀 Automation & Orchestration

- **ADF Pipelines** (parameter-driven):
  - Extract data from SQL DB & APIs
  - Load into Bronze layer
  - Trigger Databricks notebooks for transformation to Silver & Gold layers
- **Databricks Unity Catalog** used to manage schemas and tables centrally

## 🧪 Pipeline Workflow

### 🔹 High-Level ADF Pipeline Flow
<img width="400" alt="SCR-20250520-lfug" src="https://github.com/user-attachments/assets/b3328f8c-125d-48e4-bc4a-99d89d6bb1b1" />

### 🔹 Config-Driven File Processing Logic
<img width="1000" alt="SCR-20250520-lfug" src="https://github.com/user-attachments/assets/bffe776d-8cee-4ce5-86f6-561cad1d748a" />

### 🔹 Databricks Silver to Gold Notebook Flow
<img width="400" alt="SCR-20250520-lfug" src="https://github.com/user-attachments/assets/796650b9-0baf-4445-aaba-3f4815948396" />

1. **ADF Pipeline** (Parameter-driven):
   - Reads data from SQL DB & APIs
   - Uploads claims & CPT code files
   - Triggers Databricks notebooks with parameters

2. **Databricks Notebooks**:
   - `raw_to_bronze`: Ingest raw data into Delta Lake
   - `bronze_to_silver`: Apply business logic & transformations
   - `silver_to_gold`: Build reporting tables (facts/dimensions)

3. **Storage**:
   - All data stored in **Azure Data Lake Storage Gen2**
   - Delta Lake format used for all layers

## 🔐 Security Setup

| Component | Security Mechanism |
|----------|---------------------|
| SQL DB Passwords | Stored in Azure Key Vault |
| Databricks Secrets | Accessed via Secret Scope linked to Key Vault |
| ADLS Gen2 | Accessed via Service Principal with RBAC |
| Unity Catalog | Used for schema governance |

## 📈 Use Cases Enabled

- Provider-level and department-level performance dashboards
- Claims and cost analysis
- Patient engagement metrics
- Operational reporting for hospital management

## 📁 Folder Structure

```
healthcare-etl-databricks/
├── notebooks/
│   ├── raw_to_bronze.py
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   └── utils/
├── configs/
│   └── paths_config.py
├── adf_pipelines/
│   └── pipeline_jsons/         # ADF pipeline definition files
├── data/
│   ├── landing/                # CPT, Claims uploads
│   └── sample_api/             # ICD, NPI data
├── unity_catalog/
│   └── schema_definitions/
├── README.md
└── requirements.txt
```

## 🚀 How to Deploy

### Prerequisites

- Azure subscription with:
  - Databricks workspace
  - Data Lake Storage Gen2
  - Azure Key Vault
  - ADF instance
- Service Principal with access to Key Vault and ADLS

### Steps

1. Clone the repository:
   ```bash
   git clone https://github.com/Shanu85/healthcare-etl-databricks.git
   ```

2. Upload raw files (claims, CPT codes) to Landing folder in ADLS

3. Configure `paths_config.py` and ADF linked services

4. Import ADF pipelines and parameterize as needed

5. Run ADF pipeline to orchestrate full load → bronze → silver → gold

6. Access processed data via Unity Catalog or downstream reporting tools


## 📄 License

MIT License – see the [LICENSE](LICENSE) file for details.
