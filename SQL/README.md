# 🚀 Databricks SQL: From Foundational DDL to Advanced Analytical Engineering

This repository contains a collection of SQL scripts and exercises executed on **Databricks**, showcasing the ability to transform raw data into actionable business insights using Spark SQL.

## 🛠️ Skills & Knowledge

### **Core SQL Foundations**
*   **Data Definition (DDL):** Creating and managing schemas and tables within the Databricks environment.
*   **Data Manipulation (DML):** Inserting, updating, and maintaining data integrity.
*   **Data Filtering & Matching:** Utilizing `WHERE`, `BETWEEN`, `IN`, and `LIKE` for precise pattern matching and data retrieval.
*   **Data Transformation:** 
    *   Calculating derived columns (e.g., Price * Qty).
    *   Handling `NULL` values and ensuring data quality.
*   **Aggregations & Logic:** Applying `GROUP BY` with `SUM`, `AVG`, `COUNT`, `MAX`, `MIN`, and filtering results using the `HAVING` clause.

### **Advanced Analytical Engineering**
*   **Data Cleansing (Silver Layer):** Standardizing inconsistent date formats and stripping string prefixes using `REGEXP_REPLACE` and `COALESCE`.
*   **Set Operations:** Performing gap analysis using `EXCEPT` to identify missing records across datasets.
*   **Window Functions:** 
    *   **Running Totals:** Calculating cumulative revenue using `SUM() OVER`.
    *   **Delta Analysis:** Using `LAG()` to compute order-over-order performance.
    *   **Deduplication:** Isolating the latest records using `QUALIFY` with `ROW_NUMBER()`.
*   **Data Reshaping:** Transposing data dimensions using the `PIVOT` operator for regional reporting.

---

## 📂 Project Structure

### 1. `databricks-sql-dml-ddl.sql`
*   **Content:** 12 comprehensive exercises.
*   **Focus:** Covers the primary **Skills & Knowledge** section, including DDL, DML, basic transformations, and core aggregations

### 2. `sql-advanced.sql`
*   **Content:** Advanced Medallion Architecture (Bronze to Silver/Gold) workflows
*   **Focus:** Complex analytical functions, windowing, and data standardization techniques to solve real-world data engineering challenges.

---
