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
