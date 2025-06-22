# BIG-DATA-ANALYSIS

**COMPANY** : CODETECH IT SOLUTIONS

NAME : SHAIK SHAFIYA

INTERN ID : CT06DF1367

DOMAIN : DATA ANALYTICS

DURATION : 6 WEEKS

MENTOR : NEELA SANTHOSH KUMAR

Customer Transaction Dashboard with Statistical and Visual Insights

Project Overview

The Customer Transaction Dashboard project provides a complete pipeline for analyzing large-scale customer transaction data, from initial ingestion to insightful visualizations. The goal is to enable businesses to better understand customer behavior, segment audiences, and assess the performance of sales categories over time.

By leveraging the distributed processing capabilities of Apache Spark (PySpark) alongside the expressive visualization features of Seaborn and Matplotlib, this solution offers a scalable and interactive approach to business intelligence. The project mimics a real-world retail analytics scenario where understanding customer spend patterns, gender-based behaviors, and category-level performance is key to strategic decision-making.

Tools and Technologies Used

Technologies and Libraries:

Apache Spark (PySpark): The primary engine for handling and processing large-scale data. It enables distributed computing, making it efficient to clean, transform, and analyze big datasets.
Pandas: Used after Spark processing to convert data into a more manageable form for local analysis. It allows flexible reshaping, filtering, and preparation of data before visualization.
Seaborn: A powerful statistical visualization library built on Matplotlib. It's used to generate heatmaps, bar plots, and trend lines for clear and insightful representation of metrics.
Matplotlib: Handles plot rendering and fine-tuning. It allows customization of charts and is used to export high-quality figures like the revenue heatmap.

Programming Language:

Python (3.8+): The core programming language used throughout the project. Python’s ecosystem supports data manipulation, visualization, and integration with big data tools.

Platform:

Google Colab: A free, cloud-based Jupyter notebook environment that supports PySpark and Python out of the box. It allows interactive development, data visualization, and execution of big data workflows without requiring any local installation or setup.

Dataset:

File Name: sample_large_customer_transactions.csv
File Format: CSV
Data Volume: Large-scale transactional data
Key Columns:
   customer_id: Unique identifier for each customer
   timestamp: Date and time of purchase
   purchase_amount: Total amount spent in a transaction
   purchase_category: Product or category purchased
   gender: Customer gender

Project Objectives and Methodology

1. Data Ingestion and Timestamp Engineering

   The CSV file is loaded into a Spark DataFrame using read.csv() with schema inference enabled.
   The timestamp is parsed into a proper datetime format using to_timestamp.
   Two new columns year and month are extracted for time-based grouping and analysis.

2. Data Cleaning

   Any rows missing essential data (customer_id, purchase_amount, timestamp, or purchase_category) are removed.
   The resulting clean DataFrame ensures reliable analytics.

3. Core Metric Calculations

  Total Transactions: Counted using .count() on the cleaned DataFrame.
  Total Revenue: Aggregated using sum("purchase_amount").
  Average Purchase per Customer: Calculated by summing purchases per customer, then averaging those totals.

4. Gender-Based Spending Analysis

   Customers are grouped by gender.
   Aggregates include:
   Unique customer count
   Total revenue
   Average transaction value
   The results are printed and visualized using a bar chart.

5. Revenue by Purchase Category

    Transactions are grouped by purchase_category.
    Total sales are computed and sorted in descending order to highlight best-performing categories.
    Results are shown via a horizontal bar chart using Seaborn’s barplot.

6. Monthly Revenue Trend
 
    Transactions are grouped by year and month.
    Total sales per month are calculated and visualized in a line plot to reveal revenue trends over time.

7. Revenue Heatmap (Year vs. Month)

    A pivot table is created using Pandas to map monthly_sales against year and month.
    A Seaborn heatmap displays temporal revenue concentration, highlighting peak months and seasonal effects.

8. Visualization Standards

   All plots use a consistent style (whitegrid) and high-resolution settings for presentation.
   Plots are configured with tight layout adjustments to ensure clarity.
   This makes them suitable for export to reports or dashboards.

Outputs Generated

Terminal Outputs:
  Total number of transactions
  Total and average revenue
  Summary statistics by gender
  Revenue breakdown by product category
  Monthly sales figures

Visual Outputs:
  Bar Plot: Total spending by gender
  Bar Plot: Revenue by product category
  Line Plot: Monthly revenue trends
  Heatmap: Year-Month revenue distribution







