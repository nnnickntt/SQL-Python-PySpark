-- Databricks notebook source
-- MAGIC %md
-- MAGIC **1. Setup (DDL/DML)**
-- MAGIC * **Task:**
-- MAGIC   1. Create the schema `workspace.session_2_sql`.
-- MAGIC   2. Create table `session_2_sql.hw_advanced_query` with columns: `txn_id` (int), `customer_id` (int), `product` (string), `category` (string), `price` (int), `qty` (int), `txn_date` (date), and `status` (string).
-- MAGIC   3. Insert the 10 provided diverse transaction records.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.session_2_sql;

CREATE TABLE IF NOT EXISTS session_2_sql.hw_advanced_query (
  txn_id int,
  customer_id int,
  product string,
  category string,
  price int,
  qty int,
  txn_date date,
  status string
);

INSERT INTO session_2_sql.hw_advanced_query VALUES
  (1, 101, 'MacBook Pro', 'Electronics', 2000, 1, '2025-01-15', 'completed'),
  (2, 102, 'AirPods', 'Electronics', 200, 2, '2025-01-16', 'completed'),
  (3, 101, 'Coffee Mug', 'Home', 20, 4, '2025-01-17', 'completed'),
  (4, 103, 'Desk Chair', 'Home', 150, 1, '2025-01-18', 'pending'),
  (5, 104, 'Monitor Stand', 'Electronics', 50, 2, '2025-02-01', 'completed'),
  (6, 102, 'Keyboard', 'Electronics', 100, 1, '2025-02-05', 'failed'),
  (7, 105, 'Sofa', 'Home', 500, 1, '2025-02-10', 'completed'),
  (8, 101, 'Mouse', 'Electronics', 80, 1, '2025-02-15', 'completed'),
  (9, 106, 'Lamp', 'Home', 40, NULL, '2025-02-20', 'pending'),
  (10, 107, 'Tent', 'Outdoor', 300, 1, '2025-03-01', 'completed');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **2. Calculated Columns**
-- MAGIC * **Task:**
-- MAGIC   1. Query the table to select `product`, `price`, and `qty`.
-- MAGIC   2. Create a new calculated column named `total_value` by multiplying `price` and `qty`.
-- MAGIC   3. Filter out any rows where `qty` IS NULL.
-- MAGIC   4. Limit to the top 2 rows to verify your calculation.
-- MAGIC * **Expected Result:**
-- MAGIC | product | price | qty | total_value |
-- MAGIC |---|---|---|---|
-- MAGIC | MacBook Pro | 2000 | 1 | 2000 |
-- MAGIC | AirPods | 200 | 2 | 400 |

-- COMMAND ----------

select product,price,qty ,(price*qty) total_value
from session_2_sql.hw_advanced_query
where qty is not null
limit 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **3. Pattern Matching**
-- MAGIC * **Task:**
-- MAGIC   1. Select the `product` and `category` columns.
-- MAGIC   2. Use the `LIKE` operator with underscores (`_`) to find any `product` whose name is exactly 4 characters long.
-- MAGIC * **Expected Result:**
-- MAGIC | product | category |
-- MAGIC |---|---|
-- MAGIC | Sofa | Home |
-- MAGIC | Lamp | Home |
-- MAGIC | Tent | Outdoor |

-- COMMAND ----------

select product , category
from session_2_sql.hw_advanced_query
where product like '____'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **4. Complex Filtering**
-- MAGIC * **Task:**
-- MAGIC   1. Select all columns (`*`).
-- MAGIC   2. Filter where `txn_date` is BETWEEN '2025-02-01' AND '2025-02-28'.
-- MAGIC   3. Add an `AND` condition to ensure `category` is IN the list ('Electronics', 'Outdoor').
-- MAGIC * **Expected Result:**
-- MAGIC |txn_id|customer_id|product|category|price|qty|txn_date|status|
-- MAGIC |---|---|---|---|---|---|---|---|
-- MAGIC |5|104|Monitor Stand|Electronics|50|2|2025-02-01|completed|
-- MAGIC |6|102|Keyboard|Electronics|100|1|2025-02-05|failed|
-- MAGIC |8|101|Mouse|Electronics|80|1|2025-02-15|completed|

-- COMMAND ----------

-- สงสัยว่าทำไมถึงเพี้ยนครับ เพราะ  mm ใน orcale กับของ databrick คนละ logic รึเปล่าครับ
select *
from session_2_sql.hw_advanced_query
where txn_date between to_date('2025-02-01','yyyy-mm-dd') and to_date('2025-02-28','yyyy-mm-dd')
and category in ('Electronics', 'Outdoor')

-- COMMAND ----------

select *
from session_2_sql.hw_advanced_query
where txn_date between  '2025-02-01' AND '2025-02-28'
and category in ('Electronics', 'Outdoor')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **5. Multi-Column Grouping**
-- MAGIC * **Task:**
-- MAGIC   1. Group the data by BOTH `category` and `status`.
-- MAGIC   2. Count the total number of records for each grouping combination.
-- MAGIC * **Expected Result:**
-- MAGIC | category | status | txn_count |
-- MAGIC |---|---|---|
-- MAGIC | Electronics | completed | 4 |
-- MAGIC | Electronics | failed | 1 |
-- MAGIC | Home | completed | 2 |
-- MAGIC | Home | pending | 2 |
-- MAGIC | Outdoor | completed | 1 |

-- COMMAND ----------

select category ,status,count(*) txn_count 
from session_2_sql.hw_advanced_query
group by category ,status
order by category

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **6. Customer Leaderboard**
-- MAGIC * **Task:**
-- MAGIC   1. Calculate the total `SUM(price)` spent by each `customer_id`.
-- MAGIC   2. Order the results from highest to lowest.
-- MAGIC   3. Limit the results to the top 3 customers.
-- MAGIC * **Expected Result:**
-- MAGIC | customer_id | total_spent |
-- MAGIC |---|---|
-- MAGIC | 101 | 2100 |
-- MAGIC | 105 | 500 |
-- MAGIC | 107 | 300 |

-- COMMAND ----------

select customer_id,sum(price) from session_2_sql.hw_advanced_query
group by customer_id
order by sum(price) desc
limit 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **7. Finding Extremes**
-- MAGIC * **Task:**
-- MAGIC   1. Group by `category`.
-- MAGIC   2. Find the highest price `MAX(price)` and lowest price `MIN(price)` within each category.
-- MAGIC * **Expected Result:**
-- MAGIC | category | highest_price | lowest_price |
-- MAGIC |---|---|---|
-- MAGIC | Electronics | 2000 | 50 |
-- MAGIC | Home | 500 | 20 |
-- MAGIC | Outdoor | 300 | 300 |

-- COMMAND ----------

select category,max(price) max_price,min(price) min_price from session_2_sql.hw_advanced_query
group by category


-- COMMAND ----------

-- MAGIC %md
-- MAGIC **8. Pre-Filtered Aggregation**
-- MAGIC * **Task:**
-- MAGIC   1. Use a `WHERE` clause to pre-filter the data, keeping only 'completed' transactions.
-- MAGIC   2. Group the remaining data by `category`.
-- MAGIC   3. Calculate the `AVG(price)` for each category.
-- MAGIC * **Expected Result:**
-- MAGIC | category | average_price |
-- MAGIC |---|---|
-- MAGIC | Electronics | 582.5 |
-- MAGIC | Home | 260.0 |
-- MAGIC | Outdoor | 300.0 |

-- COMMAND ----------

select category,avg(price) as average_price
from session_2_sql.hw_advanced_query
where status = 'completed'
group by category

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **9. Basic HAVING Clause**
-- MAGIC * **Task:**
-- MAGIC   1. Group by `category` and calculate the `SUM(qty)`.
-- MAGIC   2. Use the `HAVING` clause to strictly show categories where the total items sold are greater than 3.
-- MAGIC * **Expected Result:**
-- MAGIC | category | total_items_sold |
-- MAGIC |---|---|
-- MAGIC | Electronics | 7 |
-- MAGIC | Home | 6 |

-- COMMAND ----------

select category,sum(qty) total_items_sold
from session_2_sql.hw_advanced_query
group by category
having sum(qty)>3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **10. Advanced HAVING (Averages)**
-- MAGIC * **Task:**
-- MAGIC   1. Group the data by `customer_id` and calculate their `AVG(price)`.
-- MAGIC   2. Use `HAVING` to only show customers whose average purchase price is strictly greater than 100.
-- MAGIC * **Expected Result:**
-- MAGIC | customer_id | avg_purchase_price |
-- MAGIC |---|---|
-- MAGIC | 101 | 700.0 |
-- MAGIC | 102 | 150.0 |
-- MAGIC | 103 | 150.0 |
-- MAGIC | 105 | 500.0 |
-- MAGIC | 107 | 300.0 |

-- COMMAND ----------

select customer_id,round(avg(price),1) as average_purchase_price
from session_2_sql.hw_advanced_query
group by customer_id
having avg(price)>100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **11. Combined Date & Aggregate Filter**
-- MAGIC * **Task:**
-- MAGIC   1. Pre-filter for transactions where `txn_date > '2025-01-15'`.
-- MAGIC   2. Group the remaining records by `customer_id`.
-- MAGIC   3. Use `HAVING COUNT(*) > 1` to find customers who made multiple purchases *after* that specific date.
-- MAGIC * **Expected Result:**
-- MAGIC | customer_id | recent_purchases |
-- MAGIC |---|---|
-- MAGIC | 101 | 2 |
-- MAGIC | 102 | 2 |

-- COMMAND ----------

select customer_id,count(*) recent_purchases from session_2_sql.hw_advanced_query
where txn_date > '2025-01-15'
group by  customer_id
HAVING COUNT(*) > 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **12. Comparative HAVING Logic (The Climax)**
-- MAGIC * **Task:**
-- MAGIC   1. Group by `category`. Select the `MAX(price)` and `MIN(price)`.
-- MAGIC   2. Use the `HAVING` clause to perform a mathematical check: only return categories where the highest priced item is at least 5 times larger than the lowest priced item (`MAX >= MIN * 5`).
-- MAGIC * **Expected Result:**
-- MAGIC | category | max_price | min_price |
-- MAGIC |---|---|---|
-- MAGIC | Electronics | 2000 | 50 |
-- MAGIC | Home | 500 | 20 |

-- COMMAND ----------

select category,max(price) max_price,min(price) min_price from session_2_sql.hw_advanced_query
group by category
having max(price) >= min(price) * 5