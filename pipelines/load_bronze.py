import duckdb
import os
import json

# Function to create medallion schema
def create_medallion_schema(conn, schema_name: str):
    conn.execute(f"""
        CREATE SCHEMA IF NOT EXISTS {schema_name};
    """)


# Function to load JSON to DuckDB
def load_json_to_duckdb(json_file: str, table_name: str, schema_name:str) -> None:
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
    # Define parameters
    endpoints = ['municipios']
    schema = "bronze"

    # Connect DuckDB
    conn = duckdb.connect('data/ibge_localidades.duckdb')

    try:
        for endpoint in endpoints:
            json_file_path = f"./data/bronze/{endpoint}.json"

            load_json_to_duckdb(json_file_path, endpoint, schema)
    finally:
        conn.close()
