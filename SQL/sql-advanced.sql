-- Databricks notebook source
CREATE OR REPLACE TABLE session_3_sql.brz_web_orders (
  order_id STRING, 
  user_ref STRING, 
  order_dt STRING, 
  raw_amount STRING, 
  device STRING
);

CREATE OR REPLACE TABLE session_3_sql.dim_user_profiles (
  user_id INT, 
  region STRING
);

INSERT INTO session_3_sql.brz_web_orders VALUES
  ('O1', 'U100', '2026-05-01', '500.50', 'Mobile'),
  ('O2', 'U100', '2026-05-05', '150.00', 'Mobile'),
  ('O3', 'U200', '20260510', '300.00', 'Desktop'),
  ('O4', 'U300', '2026/05/15', 'invalid', 'Desktop'),
  ('O1', 'U100', '2026-05-01', '500.50', 'Mobile');

INSERT INTO session_3_sql.dim_user_profiles VALUES 
  (100, 'North'), 
  (200, 'South'), 
  (400, 'West');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Silver Layer Cleansing & Deduplication**
-- MAGIC * **Task:**
-- MAGIC   1. Begin a `SELECT` statement targeting the `brz_web_orders` table. Include `order_id` and `device` as-is.
-- MAGIC   2. Apply the `DISTINCT` keyword to filter out exact duplicate rows across the dataset.
-- MAGIC   3. **Clean `user_ref`**: Use `regexp_replace()` to strip the 'U' prefix, then `CAST` the result to an `INT`. Alias this column as `user_id`.
-- MAGIC   4. **Standardize `order_dt`**: Use `COALESCE()` combined with `TRY_TO_DATE()` to safely handle three different incoming date formats ('yyyy-MM-dd', 'yyyyMMdd', and 'yyyy/MM/dd'). Alias the result as `clean_dt`.
-- MAGIC   5. **Format `raw_amount`**: Safely convert the string amount using `TRY_CAST()` to a `DECIMAL(10,2)` so invalid strings become nulls. Alias it as `amount`.
-- MAGIC * **Expected Result:**
-- MAGIC | order_id | user_id | clean_dt | amount | device |
-- MAGIC |---|---|---|---|---|
-- MAGIC | O1 | 100 | 2026-05-01 | 500.50 | Mobile |
-- MAGIC | O2 | 100 | 2026-05-05 | 150.00 | Mobile |
-- MAGIC | O3 | 200 | 2026-05-10 | 300.00 | Desktop |
-- MAGIC | O4 | 300 | 2026-05-15 | null | Desktop |

-- COMMAND ----------

select distinct
  order_id,
  cast(regexp_replace(user_ref,'U','')  as int) as user_id,
  coalesce(try_to_date(order_dt,'yyyy-MM-dd'),try_to_date(order_dt,'yyyyMMdd'),try_to_date(order_dt,'yyyy/MM/dd')) as clean_dt,
  try_cast(raw_amount as decimal(10,2)) amount,
  device
from
  session_3_sql.brz_web_orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Gap Analysis (Set Operations)**
-- MAGIC * **Task:**
-- MAGIC   1. Create a Common Table Expression (CTE) named `silver_orders`. Inside this CTE, apply the cleansing logic from the previous step to extract `user_id` and valid `amount` values.
-- MAGIC   2. Write a main query that selects `user_id` from the `dim_user_profiles` table (representing all registered users).
-- MAGIC   3. Use the `EXCEPT` set operator to subtract users who have valid order history.
-- MAGIC   4. To define the subtracted group, query the `silver_orders` CTE and select `user_id` specifically where the `amount IS NOT NULL`.
-- MAGIC * **Expected Result:**
-- MAGIC | user_id |
-- MAGIC |---|
-- MAGIC | 400 |

-- COMMAND ----------

create view silver_orders as (
    select distinct
  order_id,
  cast(regexp_replace(user_ref,'U','')  as int) as user_id,
  coalesce(try_to_date(order_dt,'yyyy-MM-dd'),try_to_date(order_dt,'yyyyMMdd'),try_to_date(order_dt,'yyyy/MM/dd')) as clean_dt,
  try_cast(raw_amount as decimal(10,2)) amount,
  device
from
  session_3_sql.brz_web_orders
)

-- COMMAND ----------

select user_id from session_3_sql.dim_user_profiles
except
select  user_id from silver_orders
where amount is not null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Data Enrichment & Conditional Logic**
-- MAGIC * **Task:**
-- MAGIC   1. Define a CTE named `silver_orders` containing the cleansed web orders dataset (using all columns from the first step).
-- MAGIC   2. In the main query, perform an `INNER JOIN` between `silver_orders` (alias `s`) and `dim_user_profiles` (alias `u`) using the `user_id` column.
-- MAGIC   3. Select all columns from the silver orders (`s.*`) alongside the `region` column from the user profiles.
-- MAGIC   4. Create a new derived column named `order_tier` using a `CASE WHEN` evaluation:
-- MAGIC      * If `amount` is greater than or equal to 400, label it 'High-Value'.
-- MAGIC      * If `amount` is `NULL`, label it 'Failed'.
-- MAGIC      * For all other scenarios, label it 'Standard'.
-- MAGIC * **Expected Result:**
-- MAGIC | order_id | user_id | clean_dt | amount | device | region | order_tier |
-- MAGIC |---|---|---|---|---|---|---|
-- MAGIC | O1 | 100 | 2026-05-01 | 500.50 | Mobile | North | High-Value |
-- MAGIC | O2 | 100 | 2026-05-05 | 150.00 | Mobile | North | Standard |
-- MAGIC | O3 | 200 | 2026-05-10 | 300.00 | Desktop | South | Standard |

-- COMMAND ----------

select
  * except (u.user_id),
  case
    WHEN s.amount >= 400 THEN 'High-Value'
    WHEN s.amount is NULL THEN 'Failed'
    ELSE 'Standard'
  end as order_tier
from
  silver_orders s,
  session_3_sql.dim_user_profiles u
where
  s.user_id = u.user_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Historical Financial Momentum (Windowing)**
-- MAGIC * **Task:**
-- MAGIC   1. Set up two sequential CTEs: `silver_orders` (for the cleansed data) and `enriched_orders` (for the joined data and `order_tier` logic from the previous step).
-- MAGIC   2. Query `order_id`, `user_id`, `clean_dt`, and `amount` from the `enriched_orders` CTE.
-- MAGIC   3. Add a new column to calculate a running revenue total. Use the window function `SUM(amount)` partitioned by `user_id` and ordered chronologically by `clean_dt` (`ASC`). Alias this as `running_revenue`.
-- MAGIC   4. Apply a `WHERE` clause to filter out any records where the `order_tier` is categorized as 'Failed'.
-- MAGIC * **Expected Result:**
-- MAGIC | order_id | user_id | clean_dt | amount | running_revenue |
-- MAGIC |---|---|---|---|---|
-- MAGIC | O1 | 100 | 2026-05-01 | 500.50 | 500.50 |
-- MAGIC | O2 | 100 | 2026-05-05 | 150.00 | 650.50 |
-- MAGIC | O3 | 200 | 2026-05-10 | 300.00 | 300.00 |

-- COMMAND ----------

with enriched_orders as (
  select
    s.*,
    u.region,
    case
      WHEN s.amount >= 400 THEN 'High-Value'
      WHEN s.amount is NULL THEN 'Failed'
      ELSE 'Standard'
    end as order_tier
  from
    silver_orders s,
    session_3_sql.dim_user_profiles u
  where
    s.user_id = u.user_id
)
select
  o.order_id,
  o.user_id,
  o.clean_dt,
  o.amount,
  sum(o.amount) over (partition by o.user_id order by o.clean_dt asc) running_revenue
from
  enriched_orders o
where o.order_tier != 'Failed'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Order-over-Order Delta (LAG)**
-- MAGIC * **Task:**
-- MAGIC   1. Keep the foundational `silver_orders` and `enriched_orders` CTEs in place.
-- MAGIC   2. Query `order_id`, `user_id`, and `amount` from `enriched_orders`.
-- MAGIC   3. Calculate a new column called `revenue_delta`. Subtract the user's *previous* order amount from their *current* order amount. Use the `LAG(amount)` function partitioned by `user_id` and ordered by `clean_dt ASC` to fetch the previous value.
-- MAGIC   4. Add a `WHERE` clause to filter the final results to only display orders where the `device` was 'Mobile'.
-- MAGIC * **Expected Result:**
-- MAGIC | order_id | user_id | amount | revenue_delta |
-- MAGIC |---|---|---|---|
-- MAGIC | O1 | 100 | 500.50 | null |
-- MAGIC | O2 | 100 | 150.00 | -350.50 |

-- COMMAND ----------

with enriched_orders as (
  select
    s.*,
    u.region,
    case
      WHEN s.amount >= 400 THEN 'High-Value'
      WHEN s.amount is NULL THEN 'Failed'
      ELSE 'Standard'
    end as order_tier
  from
    silver_orders s,
    session_3_sql.dim_user_profiles u
  where
    s.user_id = u.user_id
)
select
  o.order_id,
  o.user_id,
  o.amount,
  amount - lag(amount) over (partition by user_id order by clean_dt asc)
from
  enriched_orders o
where o.device ='Mobile'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Absolute Latest Snapshot (QUALIFY)**
-- MAGIC * **Task:**
-- MAGIC   1. Retain the `silver_orders` and `enriched_orders` CTE setup.
-- MAGIC   2. Query the `user_id`, `order_id`, and `clean_dt` columns from the `enriched_orders` dataset.
-- MAGIC   3. Use the `QUALIFY` clause to immediately filter the window function results without needing a subquery. 
-- MAGIC   4. Implement `ROW_NUMBER()` partitioned by `user_id` and ordered by `clean_dt DESC` (newest orders first). Filter specifically for row number `1` to isolate the most recent transaction per user.
-- MAGIC * **Expected Result:**
-- MAGIC | user_id | order_id | clean_dt |
-- MAGIC |---|---|---|
-- MAGIC | 100 | O2 | 2026-05-05 |
-- MAGIC | 200 | O3 | 2026-05-10 |

-- COMMAND ----------

with enriched_orders as (
  select
    s.*,
    u.region,
    case
      WHEN s.amount >= 400 THEN 'High-Value'
      WHEN s.amount is NULL THEN 'Failed'
      ELSE 'Standard'
    end as order_tier
  from
    silver_orders s,
    session_3_sql.dim_user_profiles u
  where
    s.user_id = u.user_id
)
select
  o.user_id,
  o.order_id,
  o.clean_dt
from
  enriched_orders o
qualify row_number() over (partition by user_id order by clean_dt desc) = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Regional Device Matrix (PIVOT)**
-- MAGIC * **Task:**
-- MAGIC   1. Maintain the existing `silver_orders` and `enriched_orders` CTEs.
-- MAGIC   2. Write a subquery (or derived table) that selects only the `region`, `device`, and `amount` columns from `enriched_orders`. Explicitly filter out invalid data by using `WHERE amount IS NOT NULL`.
-- MAGIC   3. Wrap this subquery inside an outer `SELECT * FROM (...)` statement.
-- MAGIC   4. Chain the `PIVOT` operator to the subquery to transpose the data format. Calculate the total revenue via `SUM(amount)` specifically `FOR device IN ('Mobile', 'Desktop')`.
-- MAGIC * **Expected Result:**
-- MAGIC | region | Mobile | Desktop |
-- MAGIC |---|---|---|
-- MAGIC | North | 650.50 | null |
-- MAGIC | South | null | 300.00 |

-- COMMAND ----------

with enriched_orders as (
  select
    s.*,
    u.region,
    case
      WHEN s.amount >= 400 THEN 'High-Value'
      WHEN s.amount is NULL THEN 'Failed'
      ELSE 'Standard'
    end as order_tier
  from
    silver_orders s,
    session_3_sql.dim_user_profiles u
  where
    s.user_id = u.user_id
)
select
  * from (select
  o.region ,o.device,o.amount
from
  enriched_orders o
where
  o.amount is not null)
pivot (sum(amount) for device in ('Mobile','Desktop'))