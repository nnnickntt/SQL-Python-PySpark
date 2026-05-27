# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

sales_raw_schema = "shop_id string, sales_amt string, date string, store_size string"
sales_raw_data = [
    {"shop_id": "1", "sales_amt": "1500.50", "date": "2026/03/07", "store_size": "Large"},
    {"shop_id": "2", "sales_amt": "invalid", "date": "2026/03/08", "store_size": ""},
    {"shop_id": "3", "sales_amt": "900.00",  "date": "2026/03/09", "store_size": None}
]

shop_raw_schema = "shop_id int, shop_name string, is_active int"
shop_raw_data = [
    {"shop_id": 1, "shop_name": "BKK-01", "is_active": 1},
    {"shop_id": 2, "shop_name": "CNX-01", "is_active": 0},
    {"shop_id": 3, "shop_name": "HKT-01", "is_active": 1}
]

pipeline_sales_df = spark.createDataFrame(sales_raw_data, schema=sales_raw_schema)
pipeline_shop_df = spark.createDataFrame(shop_raw_data, schema=shop_raw_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1: Casting & Error Handling**
# MAGIC 1. Take `pipeline_sales_df`. Cast `shop_id` to `int`.
# MAGIC 2. Try cast `sales_amt` to `decimal(10,2)`.
# MAGIC 3. Use `to_date` with `"yyyy/MM/dd"` to cast the `date` string.
# MAGIC 4. Save to a new variable `sales_cast_df`. Notice what happens to the `"invalid"` text.
# MAGIC
# MAGIC * **Expected Result:**
# MAGIC | shop_id | sales_amt | date | store_size |
# MAGIC |---|---|---|---|
# MAGIC | 1 | 1500.50 | 2026-03-07 | Large |
# MAGIC | 2 | null | 2026-03-08 |  |
# MAGIC | 3 | 900.00 | 2026-03-09 | null |

# COMMAND ----------

sales_cast_df = (
    pipeline_sales_df.select(col('shop_id').cast('int'),
                             col('sales_amt').try_cast('decimal(10,2)'),
                             to_date(col('date'),'yyyy/MM/dd').alias('date'),
                             col('store_size')
                             )
 )
sales_cast_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2: Filtering & Imputation (when/otherwise)**
# MAGIC 1. Filter `sales_cast_df` to remove rows where `sales_amt` `.isNull()`.
# MAGIC 2. Use `.withColumn` and `.when()` to replace empty strings (`""`) or `null` in `store_size` with `"Unknown"`.
# MAGIC 3. Save as `sales_clean_df`.
# MAGIC
# MAGIC * **Expected Result:**
# MAGIC | shop_id | sales_amt | date | store_size |
# MAGIC |---|---|---|---|
# MAGIC | 1 | 1500.50 | 2026-03-07 | Large |
# MAGIC | 3 | 900.00 | 2026-03-09 | Unknown |

# COMMAND ----------

sales_clean_df = (
    sales_cast_df.filter(~(col('sales_amt').isNull()))
    .withColumn('store_size',when((col("store_size") == "") | (col("store_size").isNull()), "Unknown")
        .otherwise(col("store_size"))
                )
)
sales_clean_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3: Dimension Preparation**
# MAGIC 1. Take `pipeline_shop_df`. 
# MAGIC 2. Cast `is_active` to `boolean`.
# MAGIC 3. Add a hardcoded literal column `batch_year` = `2026`.
# MAGIC 4. Save as `shop_dim_df`.
# MAGIC
# MAGIC * **Expected Result:**
# MAGIC | shop_id | shop_name | is_active | batch_year |
# MAGIC |---|---|---|---|
# MAGIC | 1 | BKK-01 | true | 2026 |
# MAGIC | 2 | CNX-01 | false | 2026 |
# MAGIC | 3 | HKT-01 | true | 2026 |

# COMMAND ----------

 shop_dim_df = pipeline_shop_df.withColumn('is_active',col('is_active').cast('boolean'))\
 .withColumn('batch_year',lit(2026))

 shop_dim_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4: Advanced GroupBy & Aggregation**
# MAGIC 1. Take `sales_cast_df` .Replace when store_size is `""` or `null` with `"Unknown"`
# MAGIC 1. Group `sales_cast_df` by `store_size`.
# MAGIC 2. Calculate the count of unique `shop_id`s (`unique_stores`).
# MAGIC 3. Calculate the sum of `sales_amt` (`total_sales`).
# MAGIC 4. Create an array of all `date`s in that group using `collect_list()`.
# MAGIC
# MAGIC * **Expected Result:**
# MAGIC |store_size|unique_stores|total_sales|date|
# MAGIC |---|---|---|---|
# MAGIC |Large|1|1500.50|["2026-03-07"]|
# MAGIC |Unknown|2|900.00|["2026-03-08","2026-03-09"]|

# COMMAND ----------

 a=(
     sales_cast_df.withColumn('store_size',when(
     (col('store_size').isNull()) | (col('store_size') ==''),"Unknown")
                              .otherwise(col('store_size')))
    .groupBy(col('store_size'))
    .agg(countDistinct(col('shop_id')).alias('unique_stores'),sum(col('sales_amt')).alias('total_sales'),collect_list(col('date'))
                       )
    )            

 a.display()