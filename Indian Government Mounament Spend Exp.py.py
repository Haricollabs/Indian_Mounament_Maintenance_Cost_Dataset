# Databricks notebook source
# MAGIC %md
# MAGIC ### Resource Pages
# MAGIC - https://www.data.gov.in/sector?page=1
# MAGIC - https://www.data.gov.in/resource/stateut-wise-allocation-expenditure-incurred-conservation-preservation-and-environmental

# COMMAND ----------

# import statements
import requests
import pandas as pd
from io import StringIO
from pyspark.sql.functions import col, lit, split, lag, lead
from pyspark.sql.functions import expr,sum
from pyspark.sql.window import Window

# COMMAND ----------

# Get API key from the secret scope
api_key=dbutils.secrets.get(scope="practice-secret-scope",key="Data_Gov_IN_APIkey")

# COMMAND ----------

# Connecting to API & Fetching Data
urlcsv=f"https://api.data.gov.in/resource/2d4bdf09-452f-446f-ac90-66d5eae15e3b?api-key={api_key}&format=csv&limit=100&offset=0"

response = requests.get(urlcsv)

if response.status_code == 200:
    df=pd.read_csv(StringIO(response.text))
else:
    print(f"Error: {response.status_code}")

# COMMAND ----------

# First level transformation
dfs=spark.createDataFrame(df)
dfs=dfs.select((dfs.columns)[1:])
# Convert the values to Lakhs
for column in dfs.columns:
    if dict(df.dtypes)[column]=='object':
        dfs=dfs.withColumn("State_or_UT", col(column).cast('string')).drop(column)
    else:    
        dfs=dfs.withColumn(column.replace(' - ','__').replace('-','_'), (col(column)*100000).cast('Decimal')).drop(column)

# COMMAND ----------

# Unpivot Alloaction and Expenditure
id_column = dfs.columns[0]
value_columns = dfs.columns[1:]

# Create expressions for each column to unpivot
unpivot_exprs = [f"stack({len(value_columns)}, " + 
                 ", ".join([f"'{col}', {col}" for col in value_columns]) + 
                 f") as (Type, Amt)"]

# Apply unpivot
unpivoted_df = dfs.selectExpr(id_column, *unpivot_exprs)
display(unpivoted_df)
#print(unpivot_exprs)

# COMMAND ----------

# Pivot to get Allocated_Amount and Spent_Amount
udfs=unpivoted_df.withColumn('Financial_Year',(split(col('Type'),"__")).getItem(0)).withColumn('Type',(split(col('Type'),"__")).getItem(1))
udfs=udfs.select("State_or_UT","Financial_Year","Type","Amt")
display(udfs)

# COMMAND ----------

# Categorise type as allocation/expenditure
pdfs=udfs.groupBy(["State_or_UT","Financial_Year"]).pivot("Type").agg(sum("Amt")).orderBy("State_or_UT","Financial_Year")
pdfs=pdfs.withColumnsRenamed({"Allocation":"Allocated_Amount","Expenditure":"Spent_Amount"})
display(pdfs)

# COMMAND ----------

# Calculate Increase/Decrease in allocation
wind=Window.partitionBy("State_or_UT").orderBy("State_or_UT","Financial_Year")
fdfs=pdfs.withColumn("Change_In_Allocation",((col("Allocated_Amount")-lag("Allocated_Amount",1).over(wind))/lag("Allocated_Amount",1).over(wind)*100).cast("Decimal"))
fdfs=fdfs.fillna(0)
display(fdfs)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Practice_Assets_Catalog.default.Mounament_spend_per_year (
# MAGIC     State_or_UT STRING,
# MAGIC     Financial_Year STRING,
# MAGIC     Allocated_Amount DECIMAL(20,0),
# MAGIC     Spent_Amount DECIMAL(20,0)
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# Write Dataframe to a Delta table
pdfs.write.mode("overwrite").format("Delta").saveAsTable("Practice_Assets_Catalog.default.Mounament_spend_per_year")
