# financial-forecasting-distributed-ml

# 🏦 Financial Forecasting Frontier: Distributed ML

An end-to-end distributed banking data engineering, exploratory analysis, predictive modeling, and real-time streaming pipeline built with **Hadoop HDFS concepts, Hive / Spark SQL, Apache Spark, Spark MLlib, and Spark Structured Streaming** on the UCI Bank Marketing dataset (`bank.csv`, 4,521 records across 17 features).

---

## 📌 Project Overview & Business Context

Modern retail banks generate high-volume customer demographics, account ledgers, loan portfolios, marketing outreach logs, and real-time transaction streams that outgrow single-machine tools. This project simulates an enterprise banking data platform across five modular stages:

1. **Data Storage & Warehousing (`Hadoop` + `Hive`):** Columnar Parquet storage and HiveQL analytical querying to replace slow row-based batch scans.
2. **Distributed Exploratory Data Analysis (`PySpark` + `Spark SQL`):** Large-scale schema profiling, quantile-based outlier detection, custom UDF segmentation, and correlation analysis.
3. **Predictive Modeling (`Spark MLlib`):** Leak-free feature engineering pipelines, multi-model classification benchmarking, and distributed `CrossValidator` hyperparameter tuning to predict term-deposit subscription (`y`).
4. **Real-Time Transaction Processing (`Spark Structured Streaming`):** Sub-second micro-batch ingestion for multi-tier fraud scoring, live transaction volume tracking, and watermarked window aggregations.
5. **Data Parallelism & Optimization:** Repartitioning, memory caching, lazy evaluation, and broadcast joins to maximize distributed cluster throughput.

---

## 📂 Repository Structure

```text
financial-forecasting-distributed-ml/
├── data/
│   └── bank.csv                           # Banking marketing dataset (4,521 records, 17 features)
├── notebooks/
│   ├── 01_Setup_and_Data_Storage.ipynb    # HDFS Parquet storage, MapReduce & HiveQL analysis
│   ├── 02_EDA_with_Spark.ipynb            # Distributed EDA, UDFs, Spark SQL & visualizations
│   ├── 03_Predictive_Modeling.ipynb       # Spark ML pipelines, LR/RF/GBT & CrossValidator tuning
│   ├── 04_Spark_Streaming.ipynb           # Structured Streaming fraud & live campaign alerts
│   └── 05_Data_Parallelism.ipynb          # Repartitioning, caching, broadcast joins & benchmarks
└── README.md
