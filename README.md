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
```
## 📊 Dataset Schema (bank.csv)

| Category | Features | Description |
| :--- | :--- | :--- |
| **Demographics** | age, job, marital, education | Customer age, profession, marital status, and education tier |
| **Financial Profile** | default, balance, housing, loan | Credit default flag, average yearly balance ($), housing & personal loans |
| **Current Campaign** | contact, day, month, duration, campaign | Communication channel, last contact date, call duration (sec), contact count |
| **Historical Campaign** | pdays, previous, poutcome | Days since prior contact (-1 = never contacted), prior contacts & outcome |
| **Target Variable** | y | Term deposit subscription (yes: 521 / 11.52%, no: 4,000 / 88.48%) |

---

## 🚀 Key Findings & Quantitative Results

### 1. Data Storage & HiveQL Warehousing (01_Setup_and_Data_Storage.ipynb)
* **Columnar Storage:** Persisted `bank.csv` into distributed Parquet partitions and queried via Hive-enabled Spark SQL.
* **Account Balance by Profession:** Retired ($2,319.19), housemaid ($2,083.80), and management ($1,766.93) hold the highest average balances; blue-collar ($1,085.16) holds the lowest.
* **Outreach Timing Paradox:** May recorded the highest outreach volume (1,398 contacts) but the lowest conversion rate (6.65%), whereas October (46.25%), December (45.00%), and March (42.86%) delivered peak conversion efficiency.
* **Engagement Impact:** Subscribed clients averaged 552.7 seconds on calls compared to 226.3 seconds for non-subscribers (2.44x longer).

### 2. Distributed EDA (02_EDA_with_Spark.ipynb)
* **Age Conversion Curve (PySpark UDF):** Clients under 25 (20.7%) and 65+ (36.9%) convert at significantly higher rates than working-age cohorts (25–64: 9.8%–11.6%).
* **Outlier & Skewness Analysis:** Spark's `approxQuantile` identified an IQR of [-$1,963, $3,461], isolating 530 high-balance outliers (11.7%) up to $71,188.
* **Credit Risk & Contact Channels:** Entrepreneurs had the highest credit default rate (4.17%), while telephone (14.62%) and cellular (14.36%) channels outperformed unknown contact channels (4.61%) by over 3x.
* **Linear Correlation:** Pearson correlation between age and balance is weak (r = 0.08), confirming the need for non-linear tree-based models.

### 3. Predictive Modeling with Spark ML (03_Predictive_Modeling.ipynb)
* **Pipeline Architecture:** 1.5× IQR Outlier Capping → StringIndexer → OneHotEncoder → VectorAssembler → StandardScaler → Distributed Classifier (80/20 split: 3,662 train / 859 test).
* **Model Comparison & 3-Fold CrossValidator Tuning:**

| Model | AUC-ROC | Accuracy | F1-Score | Precision | Recall |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Random Forest (Tuned)** | 0.9128 | 89.17% | 0.8626 | 0.8675 | 0.8917 |
| **Gradient-Boosted Trees** | 0.9051 | 90.22% | 0.8904 | 0.8880 | 0.9022 |
| **Logistic Regression** | 0.8963 | 89.87% | 0.8791 | 0.8808 | 0.8987 |

* **Top Predictive Drivers:** Call duration, account balance, client age, pdays, and poutcome_success.

### 4. Real-Time Transaction Streaming (04_Spark_Streaming.ipynb)
* **Micro-Batch Processing:** Ingested 200 real-time banking transactions across 5 transaction types (deposit, withdrawal, transfer, payment, loan_repayment) using 5-second triggers.
* **Live Fraud Detection:** Flagged high-risk anomalies (transaction_amount > $4,000 with balance < $100) at a 0.50% high-risk fraud rate, visualized alongside live channel volumes on a 4-panel monitoring dashboard.
* **Windowing & Watermarking:** Implemented time-window aggregations with `withWatermark` to bound state memory while accurately capturing late-arriving events.

### 5. Data Parallelism & Optimization (05_Data_Parallelism.ipynb)
* **Partition Scaling (`repartition(8)` vs. `coalesce(1)`):** Distributing aggregations across 8 partitions reduced `groupBy` execution time from 2.413s to 1.230s (1.96x speedup).
* **Broadcast Join Optimization:** Broadcasting the `job_risk` lookup table (`F.broadcast()`) eliminated wide network shuffles, reducing join runtime from 1.253s to 0.936s (1.34x speedup).
* **Lazy Evaluation:** Fused 6 feature-engineering transformations into a single Catalyst physical stage executing in 0.497s.

---

## 🛠️ Tech Stack & How to Run

* **Languages & Frameworks:** Python 3, Apache Spark 3.5 / 4.0 (PySpark), Spark SQL / HiveQL, Spark MLlib, Spark Structured Streaming
* **Visualization & Monitoring:** Matplotlib, Seaborn, Pandas, psutil
* **Execution Environment:** Google Colab (local[*] multi-core runtime with `.enableHiveSupport()`)

### Running the Notebooks
1. Clone this repository or open any notebook in `notebooks/` directly in Google Colab.
2. Place `data/bank.csv` in your Google Drive project folder (or upload it to `/content/bank.csv`).
3. Run the notebooks sequentially from 01 to 05 (Notebook 03 saves `saved_rf_pipeline`, which Notebook 04 loads for live streaming predictions).
