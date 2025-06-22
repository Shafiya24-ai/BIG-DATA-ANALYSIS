from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, avg, sum, countDistinct
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Initialize Spark Session 
spark = SparkSession.builder.appName("Customer Transaction Dashboard").getOrCreate()

# Load Dataset 
df = spark.read.csv("sample_large_customer_transactions.csv", header=True, inferSchema=True)

# Data Cleaning and Feature Engineeri
df = df.withColumn("timestamp", to_timestamp("timestamp"))
df = df.withColumn("year", year("timestamp")).withColumn("month", month("timestamp"))

# Drop rows with critical nulls
df_clean = df.dropna(subset=["customer_id", "purchase_amount", "timestamp", "purchase_category"])

# Metrics

# 1. Total number of transactions
total_tx = df_clean.count()
print(f"Total Transactions: {total_tx}")

# 2. Total revenue
total_revenue = df_clean.agg(sum("purchase_amount")).first()[0]
print(f"Total Revenue: ₹{total_revenue:.2f}")

# 3. Average purchase per customer
avg_per_customer = df_clean.groupBy("customer_id") \
    .agg(sum("purchase_amount").alias("total_spent")) \
    .agg(avg("total_spent")).first()[0]
print(f"Average purchase per customer: ₹{avg_per_customer:.2f}")

# 4. Gender-wise Analysis 
gender_df = df_clean.groupBy("gender").agg(
    countDistinct("customer_id").alias("unique_customers"),
    sum("purchase_amount").alias("total_spent"),
    avg("purchase_amount").alias("avg_transaction")
)
gender_df.show()

# 5. Category-wise Revenue 
category_df = df_clean.groupBy("purchase_category") \
    .agg(sum("purchase_amount").alias("total_sales")) \
    .orderBy(col("total_sales").desc())
category_df.show()

# === 6. Monthly Revenue Trend ===
monthly_df = df_clean.groupBy("year", "month") \
    .agg(sum("purchase_amount").alias("monthly_sales")) \
    .orderBy("year", "month")
monthly_df.show()

# Visualization Section

# Set style
sns.set(style="whitegrid")

# Gender-wise Spending 
pdf_gender = gender_df.toPandas()
plt.figure(figsize=(6, 4))
sns.barplot(x='gender', y='total_spent', data=pdf_gender)
plt.title("Total Spending by Gender")
plt.xlabel("Gender")
plt.ylabel("Total Revenue (₹)")
plt.tight_layout()
plt.show()

# Revenue by Category 
pdf_cat = category_df.toPandas()
plt.figure(figsize=(10, 6))
sns.barplot(y='purchase_category', x='total_sales', data=pdf_cat, palette="viridis")
plt.title("Top Categories by Revenue")
plt.xlabel("Total Revenue (₹)")
plt.ylabel("Purchase Category")
plt.tight_layout()
plt.show()

# Monthly Revenue Trend 
pdf_monthly = monthly_df.toPandas()
pdf_monthly['year_month'] = pd.to_datetime(pdf_monthly[['year', 'month']].assign(day=1))

plt.figure(figsize=(12, 5))
sns.lineplot(x='year_month', y='monthly_sales', data=pdf_monthly, marker='o')
plt.title("Monthly Revenue Trend")
plt.xlabel("Month")
plt.ylabel("Revenue (₹)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Heatmap of Revenue by Year and Month 
heatmap_df = pdf_monthly.pivot(index="year", columns="month", values="monthly_sales")
plt.figure(figsize=(10, 6))
sns.heatmap(heatmap_df, annot=True, fmt=".0f", cmap="YlGnBu", linewidths=0.5)
plt.title("Revenue Heatmap (Year vs Month)")
plt.xlabel("Month")
plt.ylabel("Year")
plt.tight_layout()
plt.show()

# Stop Spark Session 
spark.stop()
