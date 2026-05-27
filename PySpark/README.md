### 🛠️ Skills & Knowledge

**PySpark for Large-Scale Data Transformation (Practice)**

* **Data Cleaning & Type Casting:** Utilizing `trim()` to clear malformed string whitespaces, `try_cast()` for type-safe numeric conversion, and `to_date()` with specific date formats (e.g., `"yyyy/MM/dd"`) to standardize timelines.
* **Imputation & Missing Values:** Implementing `when().otherwise()` conditionals and `coalesce()` functions to safely replace empty strings (`""`) or `null` records with default values like `"Unknown"` or `"Online"`.
* **Advanced Joining Techniques:** Performing multi-table Left Joins using DataFrame Aliases and executing Non-Equi Joins based on open conditional logic (e.g., matching records where an `order_date` falls `BETWEEN` promo start and end dates).
* **Window Analytical Functions:** Defining structured data partitions using `Window.partitionBy()` and `orderBy()` to calculate `row_number()` for data deduplication and rolling `sum()` for dynamic metrics like Lifetime Value (LTV).
* **Complex Data Types & Aggregations:** Grouping large datasets via `groupBy()` to compute multi-metric aggregations like `countDistinct()` and `collect_list()`, and wrapping multi-dimensional attributes into single nested JSON objects using `struct()`.

---

### 📂 Project Structure

**1. Data-Transformation-Fundamentals.py**

* **Content:** Core PySpark data cleaning, data imputation, and categorical aggregations.
* **Focus:** Hands-on practice with schema-based DataFrame creation, handling empty/null fields using `when/otherwise`, type casting, and extracting multi-metric array lists from grouped data.

**2. Analytical-Pipeline-and-Windowing.py**

* **Content:** An end-to-end analytical ETL pipeline utilizing Spark SQL and analytical functions.
* **Focus:** Date-between Joins | Window Functions (row_number, sum) | Nested struct() | Delta Table Outputs.
