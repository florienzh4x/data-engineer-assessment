# 3. Data Visualization Assessment

## Overview

Bagian ini merupakan analisis dan visualisasi data koperasi Indonesia menggunakan Looker Studio. Dashboard dirancang untuk memberikan insights mengenai distribusi, demografi, dan karakteristik koperasi di seluruh Indonesia berdasarkan data dari Kemenkop.

## Tools Used

- Looker Studio

## Struktur Repository

    3-visualization/
    │
    ├── screenshots/
    │ └── Dashboard Koperasi Indonesia.png
    │
    └── README.md

## Dashboard Components
1. Distribution of Cooperative Types
   Chart: Stacked Bar Chart
   Purpose: Menampilkan distribusi jenis koperasi berdasarkan sektor/kelompok.
   Key Insights:
    - Jenis koperasi yang paling dominan
    - Perbandingan antar kelompok

2. Geographical Distribution of Cooperatives
    Chart Type: Google Maps
    Purpose: Visualisasi persebaran koperasi di seluruh Indonesia.
    Implementation:
    - Dimension Color: Koperasi
    - Tooltip: lat_long

3. Gender Comparison Analysis
    Chart Type: Bar Chart
    Purpose: Menampilkan perbandingan antara laki-laki dan perempuan dalam koperasi.
    Metrics Displayed:
    - Total anggota Pria
    - Total anggota Wanita

4. Top 5 Provinces with the Most Cooperatives
    Chart Type: Table
    Purpose: Menampilkan daftar 5 teratas provinsi dengan jumlah koperasi tertinggi.
    Purpose: Identifikasi provinsi dengan jumlah koperasi terbanyak
    Ranking Criteria: Total number of cooperatives in each province

## Data Preparation Steps:

##### Dataset Information
- Source Dataset: Google Sheets
- Import Method: Direct Google Sheets integration via URL
- Data Points: 5000

### 1. Data Import
- Connected Looker Studio to Google Sheets URL
- Verified data structure and field types

### 2. Data Cleaning (Looker Studio)
- Validated numeric fields (e.g., Total Anggota, Jumlah Anggota Pria, Jumlah Anggota Wanita)

## Additional Charts
1. Top 5 Kelompok Koperasi Dengan Koperasi Terbanyak
    Chart Type: Table
    Purpose: Identifikasi kelompok koperasi dengan jumlah koperasi terbanyak
    Ranking Criteria: Total number of cooperatives in each group

2. Status NIK (Nomor Induk Koperasi)
    Chart Type: Donut Chart
    Purpose: Menampilkan perbandingan antara koperasi aktif, belum daftar, dan Kadaluarsa
    Metrics Displayed:
    - Total koperasi aktif (persentase & angka)
    - Total koperasi belum daftar (persentase & angka)
    - Total koperasi kadaluarsa (persentase & angka)

## Dashboard Access
- Link: [Dashboard Koperasi Indonesia](https://lookerstudio.google.com/reporting/37813b0a-e91b-49d3-80b8-1ecf2bde3660)
- Access: View Only

## Screenshots

![](./screenshots/Dashboard%20Koperasi%20Indonesia.png)

## Time Spent
Dashboard Design: 3 jam
