# UAS Teknologi Big Data - NIM Genap

Nama: Muhammad Riduwan  
NIM: 230104040080  
Soal: NIM akhir genap - Smart Hospital Monitoring System

## Pipeline
Patient Sensor Data -> Spark Processing -> Parquet Storage -> AI Prediction -> Streamlit Dashboard

## File
- main_uas_230104040080.py
- dashboard_230104040080.py

## Cara Menjalankan di WSL VS Code

```bash
mkdir -p ~/bigdata-project/uas-tbg-230104040080
cd ~/bigdata-project/uas-tbg-230104040080
```

Copy kedua file `.py` ke folder tersebut.

Install dependency:

```bash
sudo apt update
sudo apt install -y python3-pip openjdk-17-jdk
pip install pyspark streamlit plotly scikit-learn pandas setuptools
```

Jalankan engine:

```bash
python3 main_uas_230104040080.py
```

Cek file Parquet:

```bash
ls output/
ls output/patient_total
ls output/patient_time
ls output/ml_data
```

Jalankan dashboard:

```bash
streamlit run dashboard_230104040080.py
```

## Output yang perlu di-screenshot
1. Terminal saat Parquet berhasil dibuat.
2. Dashboard Streamlit.
3. Grafik tren pasien.
4. Prediksi AI.
5. Analisis jam pasien tertinggi.
