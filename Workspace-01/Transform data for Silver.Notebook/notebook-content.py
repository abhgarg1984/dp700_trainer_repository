# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "605ee110-fdc2-43d5-8790-740821cd3d49",
# META       "default_lakehouse_name": "medallion",
# META       "default_lakehouse_workspace_id": "31f9cbf4-392e-41c7-aa92-cff7786209b2",
# META       "known_lakehouses": [
# META         {
# META           "id": "605ee110-fdc2-43d5-8790-740821cd3d49"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
    
# Create the schema for the table
orderSchema = StructType([
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderLineNumber", IntegerType()),
    StructField("OrderDate", DateType()),
    StructField("CustomerName", StringType()),
    StructField("Email", StringType()),
    StructField("Item", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", FloatType()),
    StructField("Tax", FloatType())
    ])
    
# Import all files from bronze folder of lakehouse
df = spark.read.format("csv").option("header", "false").schema(orderSchema).load("Files/bronze/*.csv")
    
# Display the first 10 rows of the dataframe to preview your data
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from csv.`abfss://DP700_Training_WS@onelake.dfs.fabric.microsoft.com/medallion.Lakehouse/Files/bronze` limit 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create Bronze Table

# CELL ********************

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC drop table if exists sales_bronze;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn("FileName",input_file_name()).write.format("delta").saveAsTable("sales_bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from sales_bronze limit 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Start Silver transformations

# CELL ********************

from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name
    
# Add columns IsFlagged, CreatedTS and ModifiedTS
df = df.withColumn("FileName", input_file_name()) \
    .withColumn("IsFlagged", when(col("OrderDate") < '2019-08-01',True).otherwise(False)) \
    .withColumn("CreatedTS", current_timestamp()).withColumn("ModifiedTS", current_timestamp())
    
# Update CustomerName to "Unknown" if CustomerName null or empty
df = df.withColumn("CustomerName", when((col("CustomerName").isNull() | (col("CustomerName")=="")),lit("Unknown")).otherwise(col("CustomerName")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC drop table if exists sales_silver;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC create TABLE sales_silver using DELTA as 
# MAGIC select 
# MAGIC SalesOrderNumber,
# MAGIC SalesOrderLineNumber,
# MAGIC OrderDate,
# MAGIC case when CustomerName = '' or CustomerName is null then 'Unknown' else CustomerName end as CustomerName,
# MAGIC Email,
# MAGIC Item,
# MAGIC Quantity,
# MAGIC UnitPrice,
# MAGIC Tax
# MAGIC ,FileName
# MAGIC ,case when OrderDate < '2019-08-01' then True else False end as IsFlagged
# MAGIC ,current_timestamp() as CreatedTS
# MAGIC ,current_timestamp() as ModifiedTS
# MAGIC from sales_bronze 
# MAGIC where 1=2;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from sales_silver limit 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # MERGE UPDATED incoming data

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC MERGE into sales_silver tgt
# MAGIC using sales_bronze src
# MAGIC on (src.SalesOrderNumber = tgt.SalesOrderNumber and src.OrderDate = tgt.OrderDate and src.CustomerName = tgt.CustomerName
# MAGIC and src.Item = tgt.Item)
# MAGIC WHEN MATCHED THEN UPDATE
# MAGIC SET tgt.ModifiedTS = CURRENT_TIMESTAMP()
# MAGIC WHEN NOT MATCHED THEN INSERT(
# MAGIC SalesOrderNumber,
# MAGIC SalesOrderLineNumber,
# MAGIC OrderDate,
# MAGIC CustomerName,
# MAGIC Email,
# MAGIC Item,
# MAGIC Quantity,
# MAGIC UnitPrice,
# MAGIC Tax,
# MAGIC FileName,
# MAGIC IsFlagged,
# MAGIC CreatedTS,
# MAGIC ModifiedTS
# MAGIC )
# MAGIC VALUES (
# MAGIC src.SalesOrderNumber,
# MAGIC src.SalesOrderLineNumber,
# MAGIC src.OrderDate,
# MAGIC case when src.CustomerName = '' or src.CustomerName is null then 'Unknown' else src.CustomerName end,
# MAGIC src.Email,
# MAGIC src.Item,
# MAGIC src.Quantity,
# MAGIC src.UnitPrice,
# MAGIC src.Tax,
# MAGIC src.FileName,
# MAGIC case when src.OrderDate < '2019-08-01' then True else False end,
# MAGIC current_timestamp(),
# MAGIC current_timestamp()
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Transform data for gold layer

# CELL ********************

# Load data to the dataframe as a starting point to create the gold layer
df = spark.read.table("dbo.sales_silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM medallion.sales_silver LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from delta.tables import*
    
# Define the schema for the dimdate_gold table
DeltaTable.createIfNotExists(spark) \
    .tableName("medallion.dimdate_gold") \
    .addColumn("OrderDate", DateType()) \
    .addColumn("Day", IntegerType()) \
    .addColumn("Month", IntegerType()) \
    .addColumn("Year", IntegerType()) \
    .addColumn("mmmyyyy", StringType()) \
    .addColumn("yyyymm", StringType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, dayofmonth, month, year, date_format
    
# Create dataframe for dimDate_gold
    
dfdimDate_gold = df.dropDuplicates(["OrderDate"]).select(col("OrderDate"), \
        dayofmonth("OrderDate").alias("Day"), \
        month("OrderDate").alias("Month"), \
        year("OrderDate").alias("Year"), \
        date_format(col("OrderDate"), "MMM-yyyy").alias("mmmyyyy"), \
        date_format(col("OrderDate"), "yyyyMM").alias("yyyymm"), \
    ).orderBy("OrderDate")

# Display the first 10 rows of the dataframe to preview your data

display(dfdimDate_gold.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
    
deltaTable = DeltaTable.forPath(spark, 'Tables/dimdate_gold')
    
dfUpdates = dfdimDate_gold
    
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.OrderDate = updates.OrderDate'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "OrderDate": "updates.OrderDate",
      "Day": "updates.Day",
      "Month": "updates.Month",
      "Year": "updates.Year",
      "mmmyyyy": "updates.mmmyyyy",
      "yyyymm": "updates.yyyymm"
    }
  ) \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

from pyspark.sql.types import *
from delta.tables import *
    
# Create customer_gold dimension delta table
DeltaTable.createIfNotExists(spark) \
    .tableName("medallion.dimcustomer_gold") \
    .addColumn("CustomerName", StringType()) \
    .addColumn("Email",  StringType()) \
    .addColumn("First", StringType()) \
    .addColumn("Last", StringType()) \
    .addColumn("CustomerID", LongType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, split
    
# Create customer_silver dataframe
    
dfdimCustomer_silver = df.dropDuplicates(["CustomerName","Email"]).select(col("CustomerName"),col("Email")) \
    .withColumn("First",split(col("CustomerName"), " ").getItem(0)) \
    .withColumn("Last",split(col("CustomerName"), " ").getItem(1)) 
    
# Display the first 10 rows of the dataframe to preview your data

display(dfdimCustomer_silver.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import monotonically_increasing_id, col, when, coalesce, max, lit
    
dfdimCustomer_temp = spark.read.table("medallion.dimCustomer_gold")
    
MAXCustomerID = dfdimCustomer_temp.select(coalesce(max(col("CustomerID")),lit(0)).alias("MAXCustomerID")).first()[0]
    
dfdimCustomer_gold = dfdimCustomer_silver.join(dfdimCustomer_temp,(dfdimCustomer_silver.CustomerName == dfdimCustomer_temp.CustomerName) & (dfdimCustomer_silver.Email == dfdimCustomer_temp.Email), "left_anti")
    
dfdimCustomer_gold = dfdimCustomer_gold.withColumn("CustomerID",monotonically_increasing_id() + MAXCustomerID + 1)

# Display the first 10 rows of the dataframe to preview your data

display(dfdimCustomer_gold.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
