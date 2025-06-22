from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import duckdb
import os

# Function to create medallion schema
def create_medallion_schema(conn, schema_name: str):
    conn.execute(f"""
        CREATE SCHEMA IF NOT EXISTS {schema_name};
    """)

# Function to transform IBGE Localidades data
def transform_localidades(spark: SparkSession):
    # Define file paths
    bronze_path = "data/bronze/municipios.json"
    silver_path = "data/silver/municipios.parquet"

    # Read bronze data from DuckDB
    bronze_df = spark.read.option("multiline", "true").json(bronze_path)

    # Build DataFrame
    municipios_df = bronze_df.select (
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

    # Verify silver folder presence. If not, creates it
    if not os.path.exists("./data/silver"):
        os.makedirs("./data/silver")

    # Save DataFrame as Parquet file
    municipios_df.write.mode("overwrite").parquet(silver_path)
    print(f"Silver layer data created in: {silver_path}")

# Main function
if __name__ == "__main__":
    # Start SparkSession
    os.environ['HADOOP_HOME'] = 'C:\\hadoop'

    spark = SparkSession.builder \
    .appName("transform_silver") \
    .getOrCreate()

    try:
        transform_localidades(spark)
    except Exception as e:
        print(f"Failed to transform silver layer: {e}")
    finally:
        spark.stop()
