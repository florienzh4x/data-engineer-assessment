# 2. ETL Assessment

## Overview

Dokumen ini berisi implementasi untuk **Soal Nomor 2** dari *Data Engineer Assessment*.  
Tujuan utama assessment ini adalah membuat ETL pipeline untuk validasi data, cleaning data, transformasi data, serta handling error. Dengan menggunakan dua dataset, yaitu dataset `Assessment Data Asset Dummy.xlsx` dan `City Indonesia.xlsx`, saya mencoba mengimplementasikan ETL pipeline tersebut dengan menggunakan Python.

ETL pipeline ini dibuat dengan tujuan utamanya adalah:

- Data Validation
  Melakukan pengecekan:
  - Required Fields
  - Validasi Format Funcloc
  - Validasi numeric range
  - Validasi referential integrity terhadap master data kota

- Data Cleaning
  Melakukan pengecekan:
  - Standardisasi format teks
  - Data Transformation
  - Data Normalization
  - Normalisasi kolom Alamat
  - Logging untuk invalid records (invalid Funcloc & mismatched city)

- Data Transformation
  Melakukan pengecekan:
  - Menentukan CityCode & RegionalCode dari master data
  - Generate Internal Site ID dengan format:
    ```
    AAA-BB-CC
    AAA: City Code
    BB: Regional Code
    CC: sequence number per city (001, 002, …)
    ```

- Output
  Menyimpan hasil ETL pipeline ke dalam file `data/output_data_asset_transformed.xlsx`

## Tools Used
- Python 3.x
- Pandas
- DuckDB
- Regex (re)
- Logging

## Struktur Repository
    2-etl-assessment/
    │
    ├── data/
    │ ├── Assessment Data Asset Dummy.xlsx
    │ ├── output_data_asset_transformed.xlsx
    │ └── City Indonesia.xlsx
    │
    ├── logs/
    │ ├── etl_processing.log
    │ └── invalid_city_records_log.csv
    │
    ├── src/
    │ └── etl.py
    │
    └── README.md

## ETL Pipeline Workflow

1. Load Data

    Mengambil 2 dataset dari folder `data/`:

    - Asset data (Assessment Data Asset Dummy.xlsx)

    - Master data kota (City Indonesia.xlsx)

2. Validation Rules
    Validasi dilakukan pada:
    - Required Fields -> Funcloc, Alamat1 - Alamat4, SiteName
    - Funcloc harus numeric & 12 digit
    - Mencatat records invalid ke file log:
        - invalid_records_log.csv
        - invalid_city_records_log.csv

3. Cleaning Process
    - Uppercase & trim whitespace semua kolom alamat
    - Normalisasi nama kota di Alamat4 → hilangkan “Kabupaten”, “Kab.”, “Kota”, special chars
    - Buat kolom flagging di data asset sebagai matched key dengan data master
    - Buat flagging1 & flagging2 untuk master data (handling kota yg punya format "(…)" )

4. Combine Data
    Proses join dilakukan dengan DuckDB dengan memanfaatkan flagging1 & flagging2:
    ```sql
    SELECT 
        *
    FROM df_asset dr
    LEFT JOIN (
        SELECT 
            *,
            flagging1 AS flagging_master
        FROM df_master
        UNION ALL 
        SELECT 
            *,
            flagging2 AS flagging_master
        FROM df_master 
        WHERE flagging2 IS NOT NULL
    ) dm ON dr.flagging = dm.flagging_master
    ```

5. Transformation Process
    Membuat kolom:
    Internal Site ID = AAA-BB-CCC
    - AAA → CityCode
    - BB → RegionalCode (padded to 2 digits)
    - CCC → sequence number tiap city berdasarkan sorting Funcloc

    Contoh:
    ```
    ADL-02-001
    AGM-02-001
    ```

## How to Run

Pre-requisite:
- Python 3.x
- pip installed

1. Install requirements

    ```bash
    pip install pandas duckdb openpyxl
    ```

2. Jalankan ETL
    Dari root directory `2-etl-assessment/`
    ```bash
    python src/etl.py
    ```

3. Hasil Output
    File `data/output_data_asset_transformed.xlsx` akan terbentuk di folder `data/` dan untuk log dapat dilihat di folder `logs/`

## Time Spent
- Development: 10-11 jam
- Testing: 2-3 jam
- Dokumentasi: 1 jam

## AI Utilization

1. Code Optimization
    - Enhancement:
        - Resorting dataframe column and take out some columns
        - Combine 2 dataframe with different schema
        - Handle proper logging format

    - Deliverable:
        - "How to resorting dataframe column and take out some columns."
        - "Suggest me how to manage 2 dataframe with different schema and dont have reference key between them. some column on one dataframe must be referenced to some column on second dataframe."
        - "Suggest me to create proper logging format."