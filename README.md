# Chicago Crimes Data Analytics Platform

## Overview
A production-grade data analytics platform built on Databricks for analyzing historical crime data from Chicago (2001-2017). The project implements a complete end-to-end data pipeline from ingestion to machine learning, utilizing Unity Catalog for data governance and a medallion architecture (Bronze → Silver → Gold) for data quality and transformation.

## Project Highlights
* **Data Volume**: 6+ million crime records spanning 2001-2017
* **Architecture**: Medallion (Bronze/Silver/Gold) with Unity Catalog governance
* **ML Models**: K-Means clustering for community crime pattern analysis
* **Tech Stack**: Databricks, Apache Spark, Delta Lake, Python, SQL, scikit-learn
* **Data Quality**: Multi-stage validation with deduplication, type conformance, and spatial recovery

## Repository Structure
```
chicago_crimes_project/
├── README.md                          # This file
├── chicago_datalake/
│   └── bronze/
│       └── crimes/
│           └── Chicago_Crimes_2001_to_2004.csv  # Raw crime data
├── scripts/
│   └── ML_ANALYSIS.ipynb             # Machine learning clustering analysis
└── chicago_crimes_workspace/         # Unity Catalog namespace
    ├── bronze/                        # Raw ingested data
    ├── silver/                        # Cleaned & transformed data
    └── gold/                          # Business-ready analytics tables
```

## Data Architecture

### Medallion Layers

**Bronze Layer** (Raw Ingestion)
* Ingests CSV files from multiple time periods (2001-2004, 2005-2007, 2008-2011, 2012-2017)
* Preserves source data lineage with metadata (_source_file, _ingest_ts)
* Schema-on-read approach for flexibility

**Silver Layer** (Cleaned & Conformed)

Key tables:
* `crimes_unioned` - Combined data from all source files
* `crimes_typed` - Type-safe transformations with date parsing and coordinate recovery
* `crimes_deduped` - Deduplicated records based on crime_id
* `crimes_conformed` - Enriched with dimension lookups (districts, wards, communities)

Transformations:
* Date parsing with COALESCE fallback strategies
* Spatial data recovery from location strings (139K+ coordinates recovered)
* Boolean standardization (arrest, domestic flags)
* District/ward/community area normalization
* IUCR and FBI code standardization

**Gold Layer** (Analytics-Ready)

Key tables:
* `fact_crimes` - Star schema fact table with dimensional foreign keys
* `ml_community_features` - ML-ready features for 77 Chicago communities
* Dimension tables: `dim_date`, `dim_crime_type`, `dim_location`, `dim_shift`, `dim_location_type`

## Key Features

### 1. Data Quality & Governance
* **Deduplication**: Removed duplicate crime records based on crime_id
* **Type Safety**: Explicit type casting with TRY_CAST to handle malformed data
* **Spatial Recovery**: Recovered 139K+ missing coordinates from location strings
* **Metadata Tracking**: Full lineage tracking across all transformation steps

### 2. Dimensional Modeling
* Star schema with `fact_crimes` at center
* 5 dimension tables for flexible analytics
* Pre-computed aggregations for performance
* Community-level metrics for 77 Chicago areas

### 3. Machine Learning Analysis

**Objective**: Identify community crime patterns using unsupervised learning

**Features** (9 dimensions per community):
* total_crimes
* arrest_rate_pct
* domestic_pct
* violent_pct
* peak_crime_hour
* yoy_change_pct
* avg_crimes_per_year
* violent_share_pct
* property_share_pct

**Methodology**:
* K-Means clustering with StandardScaler normalization
* Optimal cluster selection via Elbow Method, Silhouette Score, Davies-Bouldin Index
* PCA for dimensionality reduction and visualization
* Hierarchical clustering (dendrogram) for validation

**Results**:
* Identified distinct community crime profiles
* High-volume, low-arrest areas flagged for resource allocation
* Temporal patterns analyzed via peak_crime_hour
* Year-over-year trends tracked for each cluster

## Data Schema

### fact_crimes (Gold Layer)
| Column | Type | Description |
|--------|------|-------------|
| crime_id | BIGINT | Primary key from source |
| case_number | STRING | Chicago PD case number |
| date_key | INT | FK to dim_date |
| crime_type_key | INT | FK to dim_crime_type |
| location_key | INT | FK to dim_location |
| shift_key | INT | FK to dim_shift |
| location_type_key | INT | FK to dim_location_type |
| is_arrest | BOOLEAN | Arrest made indicator |
| is_domestic | BOOLEAN | Domestic incident indicator |
| lat_final | DOUBLE | Final latitude (recovered if needed) |
| lon_final | DOUBLE | Final longitude (recovered if needed) |
| community_area_num | INT | Chicago community area (1-77) |
| community_name | STRING | Community name |
| district_code | STRING | Police district code |
| ward_num | INT | Political ward number |

### ml_community_features (Gold Layer)
77 rows, 14 columns - Community-level aggregated features for ML analysis.

## Technology Stack

* **Platform**: Databricks on AWS
* **Storage**: Delta Lake (ACID transactions, time travel)
* **Compute**: Apache Spark (distributed processing)
* **Languages**: Python, SQL
* **ML Libraries**: scikit-learn, pandas, numpy
* **Visualization**: matplotlib, seaborn, plotly
* **Governance**: Unity Catalog

## Getting Started

### Prerequisites
* Databricks workspace with Unity Catalog enabled
* Access to chicago_crimes_workspace catalog
* SQL Warehouse or cluster with DBR 13.0+

### Setup Instructions

1. **Clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/chicago-crimes-workspace.git
cd chicago-crimes-workspace
```

2. **Configure Databricks authentication**

Create a Databricks personal access token and configure connection:
```python
DATABRICKS_CONFIG = {
    'server_hostname': 'your-workspace.cloud.databricks.com',
    'http_path': '/sql/1.0/warehouses/your-warehouse-id',
    'access_token': 'your-token-here'
}
```

3. **Upload data to Bronze layer**
* Place CSV files in `/chicago_datalake/bronze/crimes/`
* Or mount cloud storage (S3/ADLS) with crime data

4. **Run ETL pipelines**

Execute the transformation notebooks in order:
* Bronze → Silver ingestion
* Silver cleaning & conformance
* Gold dimensional modeling

5. **Run ML Analysis**

Open `ML_ANALYSIS.ipynb` and execute cells sequentially to:
* Load ml_community_features
* Perform clustering analysis
* Generate visualizations

## Usage Examples

### Query crime statistics by community
```sql
SELECT 
    community_name,
    total_crimes,
    arrest_rate_pct,
    violent_pct,
    is_high_volume_area
FROM chicago_crimes_workspace.gold.ml_community_features
ORDER BY total_crimes DESC
LIMIT 10;
```

### Analyze temporal crime patterns
```sql
SELECT 
    incident_hour,
    COUNT(*) as crime_count,
    SUM(CASE WHEN is_arrest THEN 1 ELSE 0 END) as arrest_count,
    ROUND(100.0 * SUM(CASE WHEN is_arrest THEN 1 ELSE 0 END) / COUNT(*), 2) as arrest_rate
FROM chicago_crimes_workspace.gold.fact_crimes
GROUP BY incident_hour
ORDER BY incident_hour;
```

### Connect from Python
```python
from databricks import sql
import pandas as pd

connection = sql.connect(**DATABRICKS_CONFIG)
query = "SELECT * FROM chicago_crimes_workspace.gold.ml_community_features"
cursor = connection.cursor()
cursor.execute(query)
df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
cursor.close()
connection.close()
```

## ML Model Performance

**Clustering Metrics**:
* Optimal clusters identified: 4-5 (based on Silhouette/Elbow analysis)
* Average Silhouette Score: \~0.45 (moderate cluster separation)
* Davies-Bouldin Index: <1.5 (good cluster compactness)

**Insights**:
* High-crime, low-arrest clusters identified (resource optimization targets)
* Violent vs. property crime community profiles separated
* Peak hour patterns correlate with crime type distribution

## Data Quality Considerations

**Known Limitations**:
* Districts 13 and 21 missing from dimension tables (historical administrative changes)
* Community area = 0 exists in source data (flagged but kept)
* 139K records had missing coordinates (recovered from location strings)
* Date format variations handled with multiple parsing attempts

**Validation Checks**:
* Deduplication verified on crime_id
* Type conformance checked for all numeric fields
* Spatial coordinate bounds validated (Chicago lat/lon ranges)
* Temporal consistency enforced (year extracted from date matches source year range)

## Future Enhancements

* [ ] Real-time crime data ingestion via streaming APIs
* [ ] Predictive models for crime forecasting (LSTM, Prophet)
* [ ] Geospatial clustering with DBSCAN on lat/lon
* [ ] Interactive dashboards with Lakeview/Power BI
* [ ] NLP analysis on crime descriptions
* [ ] Integration with weather data for correlation analysis
* [ ] REST API for external application access

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/YourFeature`)
3. Commit changes (`git commit -m 'Add YourFeature'`)
4. Push to branch (`git push origin feature/YourFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see LICENSE file for details.

## Contact

**Project Maintainer**: rmahmood.bsds23seecs@seecs.edu.pk  
**Organization**: SEECS, NUST  
**Databricks Workspace**: dbc-580abf4c-7d5c.cloud.databricks.com

## Acknowledgments

* Chicago Police Department for open crime data
* Databricks for the unified analytics platform
* scikit-learn community for ML libraries

---

**Last Updated**: January 2025  
**Data Coverage**: 2001-2017 (6M+ records)  
**Status**: ✅ Production-Ready
