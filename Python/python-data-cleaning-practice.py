# Databricks notebook source
raw_orders = ["  ORD100:Electronics:250.50  ", "  ORD101:Apparel:45.00  ", "  ORD102:Electronics:invalid  ", "  ORD103:Home:-10.0  "]
config = {"min_threshold": 0.0, "currency": "USD"}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sub-task 2: String Sanitization
# MAGIC Create an empty list called `cleaned_orders`. Loop through `raw_orders`, strip whitespaces, and convert them to uppercase.
# MAGIC
# MAGIC * **Expected Result:** `['ORD100:ELECTRONICS:250.50', 'ORD101:APPAREL:45.00', 'ORD102:ELECTRONICS:INVALID', 'ORD103:HOME:-10.0']`

# COMMAND ----------

cleaned_orders = list()
for i in raw_orders:
    cleaned_orders.append(i.strip().upper())
print(cleaned_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sub-task 3: Parsing and Type Conversion
# MAGIC Iterate through `cleaned_orders`. Split each string by `:`. 
# MAGIC Try to convert the price (the third element) to a float. If it fails (e.g., "INVALID"), skip that record using `continue`.
# MAGIC
# MAGIC * **Expected Result:** (Filtered list of components in memory)

# COMMAND ----------

list_task3= list()
for i in cleaned_orders:
    num = i.split(':')
    try :
        float(num[2])
        list_task3.append(num)
    except Exception as s:
        print(s)
    finally : 
        print('Update list',list_task3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sub-task 4: Business Logic Validation
# MAGIC From the `parsed_records`, remove any order where the price is less than or equal to `config["min_threshold"]`.
# MAGIC
# MAGIC * **Expected Result:** `[['ORD100', 'ELECTRONICS', 250.5], ['ORD101', 'APPAREL', 45.0]]`

# COMMAND ----------

list_task4 = [item for item in list_task3 if float(item[2]) >= config["min_threshold"]]
print (list_task4)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sub-task 5: Dictionary Transformation
# MAGIC Convert the `valid_records` list into a list of dictionaries with keys: `id`, `category`, and `price`.
# MAGIC
# MAGIC * **Expected Result:** `[{'id': 'ORD100', 'category': 'ELECTRONICS', 'price': 250.5}, {'id': 'ORD101', 'category': 'APPAREL', 'price': 45.0}]`

# COMMAND ----------

valid_records = list_task4.copy()
list_task5=list()
for i in valid_records:
    tmp={}
    tmp['id'] = i[0]
    tmp['category'] = i[1]
    tmp['price'] = i[2]
    list_task5.append(tmp)
print(list_task5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sub-task 6: Category Aggregation (Sets)
# MAGIC Extract all unique categories from `order_dicts` using a set.
# MAGIC
# MAGIC * **Expected Result:** `{'ELECTRONICS', 'APPAREL'}`

# COMMAND ----------

order_dicts = list_task5.copy()
set_task6=set()
for i in order_dicts:
    set_task6.add(i['category'])
print(set_task6)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Sub-task 7: Discount Calculation
# MAGIC For every order in the "ELECTRONICS" category, apply a 10% discount to the price. Round to 2 decimals.
# MAGIC
# MAGIC * **Expected Result:** Price for ORD100 becomes `225.45`

# COMMAND ----------

task_7=list_task5.copy()
for i in task_7:
    if i['category'] =='ELECTRONICS':
        result = round(float(i['price'])*0.9,2)
        i['price'] = result
    else : print(i,'Not ELECTRONICS Category')
print(task_7)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Sub-task 8: Summary Statistics
# MAGIC Calculate the total revenue of all orders in `order_dicts`.
# MAGIC
# MAGIC * **Expected Result:** `270.45` (225.45 + 45.0)

# COMMAND ----------

result_task8 = [float(i['price']) for i in task_7]
print(sum(result_task8))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sub-task 9: Final Reporting
# MAGIC Using a loop and `enumerate`, print a final report for each order in the format: 
# MAGIC `Item 1: [ID] - [PRICE] USD`
# MAGIC
# MAGIC * **Expected Result:**
# MAGIC `Item 1: ORD100 - 225.45 USD`
# MAGIC `Item 2: ORD101 - 45.0 USD`

# COMMAND ----------

for item,i in enumerate(task_7,1):
    print (f'Item {item} : {i["id"]} - {i["price"]} USD')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Sub-task 10: System Log & Cleanup
# MAGIC Add a `status` key to the `config` dictionary with the value `"Complete"`. Clear the `raw_orders` list to save memory.
# MAGIC
# MAGIC * **Expected Result:**
# MAGIC `config: {'min_threshold': 0.0, 'currency': 'USD', 'status': 'Complete'}`
# MAGIC `raw_orders: []`

# COMMAND ----------

# MAGIC %md
# MAGIC raw_orders = ["  ORD100:Electronics:250.50  ", "  ORD101:Apparel:45.00  ", "  ORD102:Electronics:invalid  ", "  ORD103:Home:-10.0  "]
# MAGIC config = {"min_threshold": 0.0, "currency": "USD"}

# COMMAND ----------

config['status'] = 'Complete'
raw_orders.clear()
print(config)
print(raw_orders)
