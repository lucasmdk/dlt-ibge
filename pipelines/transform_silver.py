from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import duckdb
import re
import os

# Start SparkSession
spark = SparkSession.builder \
    .appName("transform_silver") \
    .getOrCreate()

# Read bronze data from DuckDB
bronze_df = spark.read.option("multiline", "true").json("data/bronze/municipios.json")

# Function to create medallion schema
def create_medallion_schema(conn, schema_name: str):
    conn.execute(f"""
        CREATE SCHEMA IF NOT EXISTS {schema_name};
    """)

# Build DataFrame
municipios_df = bronze_df.select (\
    col("id").alias("municipio_id"),
    col("nome").alias("municipio_nome"),
    col("microrregiao.id").alias("microrregiao_id"),
    col("microrregiao.nome").alias("microrregiao_nome"),
    col("microrregiao.mesorregiao.id").alias("mesorregiao_id"),
    col("microrregiao.mesorregiao.nome").alias("mesorregiao_nome"),
    col("microrregiao.mesorregiao.UF.id").alias("uf_id"),
    col("microrregiao.mesorregiao.UF.sigla").alias("uf_sigla"),
    col("microrregiao.mesorregiao.UF.nome").alias("uf_nome"),
    col("microrregiao.mesorregiao.UF.regiao.id").alias("regiao_id"),
    col("microrregiao.mesorregiao.UF.regiao.nome").alias("regiao_nome")
)

# Save DataFrame to DuckDB
municipios_df.write \
    .format("jdbc") \
    .option("url", "jdbc:duckdb:data/ibge_localidades.duckdb") \
    .option("dbtable", "silver.estados") \
    .mode("overwrite") \
    .save()

# Save DataFrame as Parquet file
municipios_df.write.mode("overwrite").parquet("data/silver/municipios.parquet")

spark.stop()
print("Transformação concluída. Dados salvos em silver.estados")
