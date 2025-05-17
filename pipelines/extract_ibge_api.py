import dlt
from dlt.sources.helpers import requests

import os
import json


# Function to ingest IBGE data
def load_ibge_data(endpoint: str) -> None:
    # Define url
    url = f"https://servicodados.ibge.gov.br/api/v1/localidades/{endpoint}"

    # Build pipeline
    pipeline = dlt.pipeline(
        pipeline_name=f"ibge_{endpoint}",
        destination='filesystem',
        dataset_name=f"{endpoint}_data"
    )

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

# Main function
if __name__ == "__main__":
    endpoints = ['municipios']

    for endpoint in endpoints:
        load_ibge_data(endpoint)
