# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create schema bronze.teste_nytaxi

# COMMAND ----------

# MAGIC %sql
# MAGIC create table bronze.teste_nytaxi.teste
# MAGIC as 
# MAGIC select * from samples.nyctaxi.trips

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.teste_nytaxi.teste

# COMMAND ----------

#https://<your-workspace>.azuredatabricks.net/#secrets/createScope

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("akv")


# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.dlhpikadev.dfs.core.windows.net",
    dbutils.secrets.get(scope="akv", key="secretAccessKeyDataLake")
)

# COMMAND ----------

df = spark.read.format("json").load("abfss://raw@dlhpikadev.dfs.core.windows.net/IGDB/games/dcac6327-3add-5a38-9f3e-ec3954219476/")

df.display()
