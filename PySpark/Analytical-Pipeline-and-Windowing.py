# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Master Initialization
raw_schema = "order_id string, store_id string, user_id int, raw_amount string, order_date string"
raw_data = [
    ("ORD1", "S1", 100, " 1500.50 ", "2026-03-10"),
    ("ORD2", "S2", 101, "invalid", "2026-03-11"),
    ("ORD3", "S1", 102, "  900.00", "2026-03-12"),
    ("ORD4", "S3", 100, "300.25", "2026-03-13"),
    ("ORD5", "S1", 100, None, "2026-03-13")
]
raw_orders = spark.createDataFrame(raw_data, schema=raw_schema)

catalog_schema = "store_id string, location string, is_flagship int"
catalog_data = [
    ("S1", "New York", 1),
    ("S2", "London", 0)
]
store_catalog = spark.createDataFrame(catalog_data, schema=catalog_schema)

promo_schema = "promo_code string, discount_rate double, start_date string, end_date string"
promo_data = [
    ("MARCH_MADNESS", 0.10, "2026-03-01", "2026-03-15")
]
promotions = spark.createDataFrame(promo_data, schema=promo_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Clean and Cast Raw Orders
# MAGIC 1. Trim whitespace from `raw_amount`.
# MAGIC 2. Try cast `raw_amount` to a `double` named `clean_amount`.
# MAGIC 3. Drop rows where `clean_amount` is null after casting.
# MAGIC
# MAGIC * **Expected Result:**
# MAGIC |order_id|store_id|user_id|clean_amount|order_date|
# MAGIC |---|---|---|---|---|
# MAGIC |ORD1|S1|100|1500.5|2026-03-10|
# MAGIC |ORD3|S1|102|900|2026-03-12|
# MAGIC |ORD4|S3|100|300.25|2026-03-13|

# COMMAND ----------

step1_df=(
    raw_orders.select('order_id'
                      ,'store_id'
                      ,'user_id'
                      ,trim(col('raw_amount')).try_cast('double').alias('clean_amount')
                      ,'order_date')
                      .dropna(subset=(['clean_amount']))
                      )
display(step1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Left Join Store Catalog & Coalesce
# MAGIC 1. Perform a left join of `step1_df` with `store_catalog` on `store_id`.
# MAGIC 2. Use `coalesce` to replace null `location` with "Online".
# MAGIC 3. Use `coalesce` to replace null `is_flagship` with `0` and cast to boolean.
# MAGIC
# MAGIC * **Expected Result:**
# MAGIC |store_id|order_id|user_id|clean_amount|order_date|location|is_flagship|
# MAGIC |---|---|---|---|---|---|---|
# MAGIC |S1|ORD1|100|1500.5|2026-03-10|New York|true|
# MAGIC |S1|ORD3|102|900|2026-03-12|New York|true|
# MAGIC |S3|ORD4|100|300.25|2026-03-13|Online|false|

# COMMAND ----------

step2_df=(
    step1_df.alias('s').join(store_catalog.alias('c'),['store_id'],'left')
    .withColumn("location", coalesce(col("c.location"), lit("Online")))
    .withColumn("is_flagship", coalesce(col("c.is_flagship"), lit(0)).cast("boolean"))
)
step2_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Date Between Join for Promotions
# MAGIC 1. Join `step2_df` and `promotions` based on an open condition (cross logic but filtered). 
# MAGIC 2. Filter the join where `order_date` is between `start_date` and `end_date`.
# MAGIC 3. Calculate `final_amount` = `clean_amount * (1 - discount_rate)`.
# MAGIC
# MAGIC * **Expected Result:**
# MAGIC |store_id|order_id|user_id|clean_amount|order_date|location|is_flagship|promo_code|final_amount|
# MAGIC |---|---|---|---|---|---|---|---|---|
# MAGIC |S1|ORD1|100|1500.5|2026-03-10|New York|true|MARCH_MADNESS|1350.45|
# MAGIC |S1|ORD3|102|900|2026-03-12|New York|true|MARCH_MADNESS|810|
# MAGIC |S3|ORD4|100|300.25|2026-03-13|Online|false|MARCH_MADNESS|270.225|

# COMMAND ----------

step2_df.createOrReplaceTempView("step2_df")
promotions.createOrReplaceTempView("promotions")
query_step3_df="""
SELECT 
   s.*,
   p.promo_code,
   s.clean_amount * (1 - p.discount_rate) as final_amount
FROM step2_df AS s
left JOIN promotions AS p
    ON s.order_date BETWEEN p.start_date AND p.end_date
"""
step3_df = spark.sql(query_step3_df)
step3_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Window Functions
# MAGIC 1. Create a Window partitioned by `user_id` and ordered by `order_date` descending.
# MAGIC 2. Calculate the `row_number()` to find the latest order per user (row_num = 1).
# MAGIC 3. Calculate the `sum` of `final_amount` per user as `lifetime_value`.
# MAGIC
# MAGIC * **Expected Result:**
# MAGIC |store_id|order_id|user_id|clean_amount|order_date|location|is_flagship|promo_code|final_amount|latest_order_rank|lifetime_value|
# MAGIC |---|---|---|---|---|---|---|---|---|---|---|
# MAGIC |S3|ORD4|100|300.25|2026-03-13|Online|false|MARCH_MADNESS|270.225|1|1620.6750000000002|
# MAGIC |S1|ORD1|100|1500.5|2026-03-10|New York|true|MARCH_MADNESS|1350.45|2|1620.6750000000002|
# MAGIC |S1|ORD3|102|900|2026-03-12|New York|true|MARCH_MADNESS|810|1|810|

# COMMAND ----------

window = Window.partitionBy('user_id').orderBy('order_date')
window_sum = Window.partitionBy('user_id')
step4_df =(
    step3_df.withColumn('latest_order_rank',row_number().over(window))
    .withColumn('lifetime_value',sum('final_amount').over(window_sum))
    .orderBy('user_id')
)
step4_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Struct Generation and Writing Output
# MAGIC 1. Filter `step4_df` for only `latest_order_rank == 1`.
# MAGIC 2. Create a struct named `user_profile` containing `location`, `lifetime_value`, and `is_flagship`.
# MAGIC 3. Write the final DataFrame to table name `session_7_spark.homework_final` with option `overwriteSchema`.
# MAGIC
# MAGIC * **Expected Result:**
# MAGIC |user_id|order_id|user_profile|
# MAGIC |---|---|---|
# MAGIC |100|ORD4|{"location":"Online","lifetime_value":1620.6750000000002,"is_flagship":false}|
# MAGIC |102|ORD3|{"location":"New York","lifetime_value":810,"is_flagship":true}|

# COMMAND ----------

step5_df=(
    step4_df.filter(col('latest_order_rank')==1)
    .select("user_id",'order_id',struct(col("location"),col("lifetime_value"),(col("is_flagship"))).alias('user_profile'))
)
step5_df.display()

step5_df.write.mode('overwrite').option('overwriteSchema',True).saveAsTable('session_7_spark.homework_final')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from session_7_spark.homework_final