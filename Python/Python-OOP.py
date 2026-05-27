# Databricks notebook source
# MAGIC %md
# MAGIC **Step 1: Object-Oriented Setup**
# MAGIC 1. Import `@dataclass`.
# MAGIC 2. Create a `@dataclass` named `SalesPipeline`.
# MAGIC 3. Add two attributes: `pipeline_name` (str) and `raw_data` (list).
# MAGIC 4. Define `__post_init__(self)` to automatically convert `pipeline_name` to uppercase.
# MAGIC
# MAGIC **Step 2: Dynamic Functions (`*args`)**
# MAGIC 1. Inside the class, define a method `ingest_batches(self, *args)`. 
# MAGIC 2. The `*args` will be multiple lists of transaction dictionaries. Loop through `args` and extend `self.raw_data` with these new lists.
# MAGIC
# MAGIC **Step 3: Comprehensions (List & Dict)**
# MAGIC 1. Define a method `extract_successful_sales(self)`.
# MAGIC 2. First, use a **list comprehension** to filter `self.raw_data`, keeping only records where the `"status"` is exactly `"success"`.
# MAGIC 3. Then, use a **dictionary comprehension** on that filtered list to return a dictionary where the key is `"txn_id"` and the value is `"amount"`.
# MAGIC
# MAGIC **Step 4: Error Handling (`try/except`)**
# MAGIC 1. Define a method `calculate_average_sale(self)`.
# MAGIC 2. Inside this method, call `self.extract_successful_sales()` to get your clean dictionary.
# MAGIC 3. Use a `try/except` block to calculate the average sale amount (`sum` of values divided by the `len` of values).
# MAGIC 4. Catch a `ZeroDivisionError` (if there are no successful sales) and print `"Warning: No successful transactions to average."`
# MAGIC 5. Catch a generic `Exception` (in case a string gets passed as an amount instead of a number) and print the specific error message. Return the average (or 0 if it fails).

# COMMAND ----------

from dataclasses import dataclass

@dataclass
class SalesPipeline:
    pipeline_name: str
    raw_data: list
    
    def __post_init__(self):
        # Step 1: Format attribute on creation
        self.pipeline_name = self.pipeline_name.upper()
        
    #Pack to tuple => ( [{data1},{data2}] , [{data3},{data4}] )
    def ingest_batches(self, *args):
        # Step 2: Handle dynamic positional arguments
        for data in args:
            #extend (drop [] out)
            self.raw_data.extend(data)
        print(self.raw_data)
        
    def extract_successful_sales(self):
        # Step 3a: List Comprehension with filtering
        # Note: using .get() prevents KeyError if "status" is missing from a corrupt record
        data_ingest = [
            i
            for i in self.raw_data
            if i.get('status')=='success'
        ]
        #print(self.data_ingest)
        
        # Step 3b: Dict Comprehension for transformation
        data_cleaned ={
            i['txn_id']:i['amount']
            for i in data_ingest
        }
        #print(data_cleaned)
        return data_cleaned
        
    def calculate_average_sale(self):
        # Step 4: Error Handling
        clean_sales = self.extract_successful_sales()
        sales_amounts = list(clean_sales.values())
        #print(sales_amounts)
        
        try:
            # Attempt math that could fail (ZeroDivision or TypeError)
            avg = sum(sales_amounts) / len(sales_amounts)
            print(f"[{self.pipeline_name}] Average Sale: {avg:.2f}")
            return avg
            
        except ZeroDivisionError:
            print(f"[{self.pipeline_name}] Warning: No successful transactions to average.")
            return 0.0
            
        except Exception as e:
            print(f"[{self.pipeline_name}] Data Error: Could not calculate average. Details: {e}")
            return 0.0

        

# COMMAND ----------

# DBTITLE 1,test result
# Initialize empty pipeline
pipeline = SalesPipeline("shopee_daily_ingest", [])

# Mock Data Batches
batch_1 = [
    {"txn_id": "T001", "amount": 150.50, "status": "success"},
    {"txn_id": "T002", "amount": 20.00, "status": "failed"}
]
batch_2 = [
    {"txn_id": "T003", "amount": 300.00, "status": "success"},
    {"txn_id": "T004", "amount": "invalid_string", "status": "success"} # This will trigger the Exception!
]

# Test 1: Ingest Data using *args
pipeline.ingest_batches(batch_1, batch_2)

# Test 2: Calculate Average (Will fail due to "invalid_string" in amount)
pipeline.calculate_average_sale()

# Test 3: Test ZeroDivisionError handling by resetting data to only failed transactions
pipeline.raw_data = [{"txn_id": "T005", "amount": 100, "status": "failed"}]
pipeline.calculate_average_sale()