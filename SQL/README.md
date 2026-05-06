# 🚀 Databricks SQL: From Foundational DDL to Advanced Analytical Engineering

This repository contains a collection of SQL scripts and exercises executed on **Databricks**, showcasing the ability to transform raw data into actionable business insights using Spark SQL.

## 🛠️ Skills & Knowledge

### **Core SQL Foundations**
*   **Data Definition (DDL):** Creating and managing schemas and tables within the Databricks environment[cite: 1].
*   **Data Manipulation (DML):** Inserting, updating, and maintaining data integrity[cite: 1].
*   **Data Filtering & Matching:** Utilizing `WHERE`, `BETWEEN`, `IN`, and `LIKE` for precise pattern matching and data retrieval[cite: 1].
*   **Data Transformation:** 
    *   Calculating derived columns (e.g., Price * Qty)[cite: 1].
    *   Handling `NULL` values and ensuring data quality[cite: 1].
*   **Aggregations & Logic:** Applying `GROUP BY` with `SUM`, `AVG`, `COUNT`, `MAX`, `MIN`, and filtering results using the `HAVING` clause[cite: 1].

### **Advanced Analytical Engineering**
*   **Data Cleansing (Silver Layer):** Standardizing inconsistent date formats and stripping string prefixes using `REGEXP_REPLACE` and `COALESCE`[cite: 1].
*   **Set Operations:** Performing gap analysis using `EXCEPT` to identify missing records across datasets[cite: 1].
*   **Window Functions:** 
    *   **Running Totals:** Calculating cumulative revenue using `SUM() OVER`[cite: 1].
    *   **Delta Analysis:** Using `LAG()` to compute order-over-order performance[cite: 1].
    *   **Deduplication:** Isolating the latest records using `QUALIFY` with `ROW_NUMBER()`[cite: 1].
*   **Data Reshaping:** Transposing data dimensions using the `PIVOT` operator for regional reporting[cite: 1].

---

## 📂 Project Structure

### 1. `databricks-sql-dml-ddl.sql`
*   **Content:** 12 comprehensive exercises[cite: 1].
*   **Focus:** Covers the primary **Skills & Knowledge** section, including DDL, DML, basic transformations, and core aggregations[cite: 1].

### 2. `sql-advanced.sql`
*   **Content:** Advanced Medallion Architecture (Bronze to Silver/Gold) workflows[cite: 1].
*   **Focus:** Complex analytical functions, windowing, and data standardization techniques to solve real-world data engineering challenges[cite: 1].

---
