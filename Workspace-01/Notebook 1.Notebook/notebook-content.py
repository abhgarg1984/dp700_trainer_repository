# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d8ae8297-047e-4551-8057-ea700a4b7df5",
# META       "default_lakehouse_name": "lakehouse",
# META       "default_lakehouse_workspace_id": "31f9cbf4-392e-41c7-aa92-cff7786209b2",
# META       "known_lakehouses": [
# META         {
# META           "id": "d8ae8297-047e-4551-8057-ea700a4b7df5"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Files/<folder>/<file>


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
df = spark.read.format("csv").load("abfss://DAY01_WS@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Files/Sales_Raw_Files/Sales.csv")
display(df.limit(100))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lakehouse.sales LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
