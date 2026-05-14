# Chicago Crimes Data Analytics Platform

[![Language: Python](https://img.shields.io/badge/language-Python-blue.svg)](https://www.python.org/)
[![Framework: Apache Spark](https://img.shields.io/badge/framework-Apache%20Spark-orange.svg)](https://spark.apache.org/)
[![Platform: Databricks](https://img.shields.io/badge/platform-Databricks-red.svg)](https://databricks.com/)
[![Storage: Delta Lake](https://img.shields.io/badge/storage-Delta%20Lake-green.svg)](https://delta.io/)
[![ML: scikit-learn](https://img.shields.io/badge/ML-scikit--learn-yellow.svg)](https://scikit-learn.org/)
[![Build Status](https://github.com/YOUR_USERNAME/chicago-crimes-workspace/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/chicago-crimes-workspace/actions)
[![Coverage Status](https://coveralls.io/repos/github/YOUR_USERNAME/chicago-crimes-workspace/badge.svg?branch=main)](https://coveralls.io/github/YOUR_USERNAME/chicago-crimes-workspace?branch=main)

---

## 📌 Overview
A **production-grade data analytics platform** built on **Databricks** for analyzing historical crime data from Chicago (2001–2017).  
Implements a complete **end-to-end data pipeline** (ingestion → transformation → machine learning) with **Unity Catalog** for governance and a **Medallion architecture** (Bronze → Silver → Gold) ensuring data quality and reliability.

---

## 🚀 Project Highlights
- **Data Volume**: 6M+ crime records spanning 2001–2017  
- **Architecture**: Medallion (Bronze/Silver/Gold) with Unity Catalog governance  
- **ML Models**: K-Means clustering for community crime pattern analysis  
- **Tech Stack**: Databricks, Apache Spark, Delta Lake, Python, SQL, scikit-learn  
- **Data Quality**: Multi-stage validation (deduplication, type conformance, spatial recovery)

---

## 📂 Repository Structure
```
chicago_crimes_project/
├── README.md
├── chicago_datalake/
│   └── bronze/
│       └── crimes/
│           └── Chicago_Crimes_2001_to_2004.csv   # Raw crime data
├── scripts/
│   └── ML_ANALYSIS.ipynb                         # Machine learning clustering analysis
└── chicago_crimes_workspace/                     # Unity Catalog namespace
    ├── bronze/                                   # Raw ingested data
    ├── silver/                                   # Cleaned & transformed data
    └── gold/                                     # Business-ready analytics tables
```

---

## 🏗️ Data Architecture

### 🔹 Bronze Layer (Raw Ingestion)
- Ingests CSV files from multiple time periods (2001–2017)  
- Preserves lineage with metadata (`_source_file`, `_ingest_ts`)  
- Schema-on-read for flexibility  

### 🔹 Silver Layer (Cleaned & Conformed)
- **Tables**: `crimes_unioned`, `crimes_typed`, `crimes_deduped`, `crimes_conformed`  
- **Transformations**:  
  - Date parsing with fallback strategies  
  - Spatial recovery (139K+ coordinates)  
  - Boolean standardization (arrest, domestic flags)  
  - District/ward/community normalization  
  - IUCR & FBI code standardization  

### 🔹 Gold Layer (Analytics-Ready)
- **Tables**:  
  - `fact_crimes` – Star schema fact table  
  - `ml_community_features` – ML-ready features for 77 communities  
  - Dimensions: `dim_date`, `dim_crime_type`, `dim_location`, `dim_shift`, `dim_location_type`

---

## 🖼️ Data Architecture Diagram
![Chicago Crimes Schema](docs/images/chicago_crimes_schema.png)

---

## 📑 Data Schema

### fact_crimes (Gold Layer)
| Column            | Type    | Description                                |
|-------------------|---------|--------------------------------------------|
| crime_id          | BIGINT  | Primary key from source                     |
| case_number       | STRING  | Chicago PD case number                      |
| date_key          | INT     | FK to dim_date                              |
| crime_type_key    | INT     | FK to dim_crime_type                        |
| location_key      | INT     | FK to dim_location                          |
| shift_key         | INT     | FK to dim_shift                             |
| location_type_key | INT     | FK to dim_location_type                     |
| is_arrest         | BOOLEAN | Arrest made indicator                       |
| is_domestic       | BOOLEAN | Domestic incident indicator                 |
| lat_final         | DOUBLE  | Final latitude (recovered if needed)        |
| lon_final         | DOUBLE  | Final longitude (recovered if needed)       |
| community_area_num| INT     | Chicago community area (1–77)               |
| community_name    | STRING  | Community name                              |
| district_code     | STRING  | Police district code                        |
| ward_num          | INT     | Political ward number                       |

### ml_community_features (Gold Layer)
77 rows, 14 columns — aggregated features for ML analysis.

---

## 📊 ML Model Performance
- **Clusters Identified**: 4–5  
- **Silhouette Score**: ~0.45 (moderate separation)  
- **Davies-Bouldin Index**: <1.5 (good compactness)  
- **Insights**:  
  - High-crime, low-arrest clusters flagged for resource allocation  
  - Violent vs. property crime profiles separated  
  - Temporal crime patterns analyzed  

---

## ⚙️ Technology Stack
- **Platform**: Databricks (AWS)  
- **Storage**: Delta Lake (ACID, time travel)  
- **Compute**: Apache Spark  
- **Languages**: Python, SQL  
- **ML Libraries**: scikit-learn, pandas, numpy  
- **Visualization**: matplotlib, seaborn, plotly  
- **Governance**: Unity Catalog  

---

## 📥 Getting Started
1. **Clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/chicago-crimes-workspace.git
   cd chicago-crimes-workspace
   ```
2. **Configure Databricks authentication**  
3. **Upload data to Bronze layer**  
4. **Run ETL pipelines (Bronze → Silver → Gold)**  
5. **Run ML Analysis (`ML_ANALYSIS.ipynb`)**

---

## 🔮 Future Enhancements
- [ ] Real-time ingestion via streaming APIs  
- [ ] Predictive models (LSTM, Prophet)  
- [ ] Geospatial clustering (DBSCAN)  
- [ ] Interactive dashboards (Lakeview/Power BI)  
- [ ] NLP on crime descriptions  
- [ ] Weather-crime correlation analysis  
- [ ] REST API for external access  

---

## 🤝 Contributing
1. Fork the repo  
2. Create a feature branch (`git checkout -b feature/YourFeature`)  
3. Commit changes (`git commit -m 'Add YourFeature'`)  
4. Push branch (`git push origin feature/YourFeature`)  
5. Open a Pull Request  

---

## 📜 License
MIT License – see LICENSE file for details.


## 📧 Contact
**Maintainer**: rmahmood.bsds23seecs
