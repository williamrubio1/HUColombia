# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import expr

# Ubicación de archivo
file_location = "/FileStore/tables/data_positivos.csv"
file_type = "csv"
##Nomre de la tabla
table_name = "data_positivos_csv"
##Reemplace "BELLO" por municipio de consulta
mun_consulta = "BELLO"

##Lee archivo csv
data = spark.read.csv(file_location, header = True, inferSchema = True)
##Query consulta
query = (f"SELECT * FROM { table_name } WHERE Municipio = '{ mun_consulta }';")

##Ejecuta Query
query_df = spark.sql(query)
##Muestra consulta
query_df.show() 

