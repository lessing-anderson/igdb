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

df1 = spark.read.format("json").load("/Volumes/bronze/raw/raw/IGDB/games/dcac6327-3add-5a38-9f3e-ec3954219476/")

df1.display()

# COMMAND ----------

df1.select("id").count() # 2676


# COMMAND ----------

df1.select("id").distinct().count() # 2676

# COMMAND ----------

df2 = spark.read.format("json").load("/Volumes/bronze/raw/raw/IGDB/games/f78b38ca-2850-5171-8924-31b381cffdd6/")

df2.display()

# COMMAND ----------

df2.select("id").count() # 301323

# COMMAND ----------

df2.select("id").distinct().count() # 301315

# COMMAND ----------

df1.union(df2).count() #303999

# COMMAND ----------

df1.union(df2).distinct().count() #301426
