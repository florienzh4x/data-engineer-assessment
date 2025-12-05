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
    │   ├── queries/
    │   │   ├── 001-ddl_query.sql         # Database schema
    │   │   ├── 002-mock-data.sql         # Generated mock data
    │   │   ├── inventory-turnover.sql         # Inventory turnover
    │   │   └── staff-performance-and-sales.sql   # Staff performance
    │   ├── results/
    │   │   ├── inventory-turnover.csv        # Inventory turnover result
    │   │   ├── inventory-turnover.png        # Inventory turnover screenshot
    │   │   ├── staff-performance-and-sales.csv  # Staff performance result
    │   │   └── staff-performance-and-sales.png  # Staff performance screenshot
    │
    ├── 2-etl-assessment/
    │   ├── README.md                      # ETL documentation
    │   ├── data/
    │   │   ├── City Indonesia.xlsx        # Master data
    │   │   ├── Assessment Data Asset Dummy.xlsx  # Raw data
    │   │   └── output_data_asset_transformed.xlsx  # Output data
    │   ├── logs/
    │   │   ├── etl_processing.log         # ETL log
    │   │   └── invalid_city_records_log.csv  # Invalid City records log
    │   ├── src/
    │       └── etl.py                     # ETL script
    │
    └── 3-data-visualization/
        ├── README.md                      # Visualization documentation
        └── screenshots/
            └── Dashboard Koperasi Indonesia.png  # Dashboard screenshot


## Setup Instructions

#### Prerequisites

```bash
# Python 3.10 or higher
python --version

# Pip package manager
pip --version

# Docker
docker --version
```

1. Clone the repository:
   ```bash
   git clone https://github.com/florienzh4x/data-engineer-assessment.git
   cd data-engineer-assessment
   ```

2. SQL Assessment Setup
   ```bash
   cd 1-sql-assessment

   # Running PostgreSQL
   docker compose up -d

   # Jalankan query di DBeaver
   ```

3. ETL Assessment Setup
   ```bash
   cd 2-etl-assessment

   # Install Dependencies
   pip install pandas duckdb openpyxl

   # Run ETL Pipeline
   python src/etl.py
   ```

4. Data Visualization
   ```bash
   cd 3-data-visualization
   # Buka link yang di tampilkan di README.md
   ```

## Time Spent

- SQL Assessment: 2-3 jam
- ETL Assessment: 13-14 jam
- Data Visualization: 3 jam

## AI Utilization

1. Mock Data Generator:
    - Build: 
        - Utilize AI to create SQL Query for mock data generation.
    - Deliverable: 
        - "Give me a SQL Query that generates mock data like 50 rows for each table. Try make it based on this schema tables: (_Given DDL Schema Query_)"

2. Code Optimization
    - Enhancement:
        - Resorting dataframe column and take out some columns
        - Combine 2 dataframe with different schema
        - Handle proper logging format

    - Deliverable:
        - "How to resorting dataframe column and take out some columns."
        - "Suggest me how to manage 2 dataframe with different schema and dont have reference key between them. some column on one dataframe must be referenced to some column on second dataframe."
        - "Suggest me to create proper logging format."

## Video Presentation

[![](https://img.youtube.com/vi/JnlCHHky2NQ/0.jpg)](https://www.youtube.com/watch?v=JnlCHHky2NQ)