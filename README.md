# Data Engineer Technical Assessment

## Overview

Repositori ini berisi submission untuk Technical Assessment posisi Data Engineer (Jr/Mid) di TAN Digital. Assessment mencakup berbagai aspek data engineering mulai dari SQL querying, ETL pipeline, hingga data visualization.

- Nama: Hasanal Bulkiah
- Email: hasanal.bulkiah@gmail.com
- Linkedin: linkedin.com/in/hasanal-bulkiah

##Assessment Scope
Dokumen ini mencakup solusi untuk:

✅ SQL Assessment - Database querying dan data analysis
✅ ETL Assessment - Data validation, cleaning, dan transformation
✅ Data Visualization - Interactive dashboard menggunakan Looker Studio
⬜ Big Data Pipeline - (Optional)

## Tools Used

- PostgreSQL
- Docker
- DBeaver
- Looker Studio
- Python
- Pandas

## Repository Structure

    data-engineer-assessment/
    │
    ├── README.md                          # This file
    │
    ├── 1-sql-assessment/
    │   ├── README.md                      # SQL documentation
    │   ├── schema.sql                     # Database schema
    │   ├── mock_data.sql                  # Generated mock data
    │   ├── queries/
    │   │   ├── basic_query_1.sql         # Inventory turnover
    │   │   ├── basic_query_2.sql         # Staff performance
    │   │   └── intermediate_query_1.sql   # Purchase patterns (optional)
    │   └── screenshots/
    │       └── query_results.png
    │
    ├── 2-etl-assessment/
    │   ├── README.md                      # ETL documentation
    │   ├── data/
    │   │   ├── City Indonesia.xlsx        # Master data
    │   │   ├── Assessment Data Asset Dummy.xlsx  # Raw data
    │   │   └── output/
    │   │       ├── cleaned_data.xlsx      # Final output
    │   │       └── error_log.csv          # Invalid records
    │   ├── etl_pipeline.py                # Main ETL script
    │   ├── requirements.txt               # Python dependencies
    │   └── notebooks/
    │       └── etl_exploration.ipynb      # Development notebook (optional)
    │
    ├── 3-data-visualization/
    │   ├── README.md                      # Visualization documentation
    │   ├── data/
    │   │   └── koperasi_indonesia.csv     # Dataset (if downloaded)
    │   ├── dashboard_link.txt             # Looker Studio URL
    │   └── screenshots/
    │       ├── dashboard_overview.png
    │       ├── map_visualization.png
    │       └── key_metrics.png
    │
    └── 4-big-data-pipeline/              # (Optional)
        ├── README.md
        ├── docker-compose.yml
        ├── src/
        ├── generator/
        └── ...