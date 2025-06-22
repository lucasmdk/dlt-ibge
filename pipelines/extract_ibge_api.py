import dlt
from dlt.sources.helpers import requests
import duckdb
import os
import json


# Function to ingest IBGE data
def load_ibge_data(endpoint: str) -> None:
    # Define url
    url = f"https://servicodados.ibge.gov.br/api/v1/localidades/{endpoint}"

    # # Build pipeline
    # pipeline = dlt.pipeline(
    #     pipeline_name=f"ibge_{endpoint}",
    #     destination='filesystem',
    #     dataset_name=f"{endpoint}_data"
    # )

    # Create request
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Verify bronze folder presence. If not, creates it
    if not os.path.exists("./data/bronze"):
        os.makedirs("./data/bronze")
    
    # Define target location
    file_path = os.path.join("./data/bronze", f"{endpoint}.json")

    # Load json data
    with open(file_path, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)

    print(f"Data from {endpoint} saved in: {file_path}")

# Function to create medallion schema
def create_medallion_schema(conn: duckdb.DuckDBPyConnection, schema_name: str):
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

# Function to load JSON to DuckDB
def load_json_to_duckdb(conn: duckdb.DuckDBPyConnection, json_file: str, table_name: str, schema_name:str) -> None:
    # Create medallion schema
    create_medallion_schema(conn, schema_name)

    # Load JSON
    if not os.path.exists(json_file):
        print(f"Arquivo {json_file} não encontrado.")
        return

    # Create DuckDB table from JSON
    conn.execute(f"""
        CREATE OR REPLACE TABLE {schema_name}.{table_name} AS 
        SELECT * FROM read_json_auto('{json_file}')
    """)

    print(f"Data from {json_file} load into table {schema_name}.{table_name} in DuckDB.")

# Main function
if __name__ == "__main__":
    endpoints = ['municipios']
    schema = "bronze"

    # Connect DuckDB
    conn = duckdb.connect('data/ibge_localidades.duckdb')

    try:
        for endpoint in endpoints:
            json_file_path = f"./data/bronze/{endpoint}.json"

            load_ibge_data(endpoint)
            load_json_to_duckdb(conn, json_file_path, endpoint, schema)
    finally:
        conn.close()
