from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import duckdb
import re
import os

# JAR path
jdb_jar_path = os.path.abspath("drivers/duckdb_jdbc-0.10.2.jar")

# Start SparkSession
spark = SparkSession.builder \
    .appName("transform_silver") \
    .config("spark.jars", jdb_jar_path) \
    .getOrCreate()

# Read bronze data from DuckDB
bronze_df = spark.read.format("jdbc") \
    .option("url", "jdbc:duckdb:data/ibge_localidades.duckdb") \
    .option("dbtable", "bronze.estados") \
    .load()

# Function to create medallion schema
def create_medallion_schema(conn, schema_name: str):
    conn.execute(f"""
        CREATE SCHEMA IF NOT EXISTS {schema_name};
    """)

# Function to extract nested data
def extract_from_bronze(field: str, key: str) -> str:
    if not field:
        return None
    match = re.search(fr"{key}=([^,}}]+)", field)
    return match.group(1).strip() if match else None

# User Defined Functions to extract fields
extract_id_udf = udf(lambda x: extract_from_bronze(x, "id"), StringType())
extract_nome_udf = udf(lambda x: extract_from_bronze(x, "nome"), StringType())
extract_sigla_udf = udf(lambda x: extract_from_bronze(x, "sigla"), StringType())

# Build DataFrame
municipios_df = bronze_df \
    .withColumn("municipio_id", col("id")) \
    .withColumn("municipio_nome", col("nome")) \
    .withColumn("microrregiao_id", extract_id_udf(col("microrregiao"))) \
    .withColumn("microrregiao_nome", extract_nome_udf(col("microrregiao"))) \
    .withColumn("mesorregiao_id", extract_id_udf(col("mesorregiao"))) \
    .withColumn("mesorregiao_nome", extract_nome_udf(col("mesorregiao"))) \
    .withColumn("uf_id", extract_id_udf(col("uf"))) \
    .withColumn("uf_sigla", extract_sigla_udf(col("uf"))) \
    .withColumn("uf_nome", extract_nome_udf(col("uf"))) \
    .withColumn("regiao_id", extract_id_udf(col("regiao"))) \
    .withColumn("regiao_nome", extract_nome_udf(col("regiao")))

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
