import logging
import os
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ais-collision-detection")
dataset = os.getenv("LIVE_DATASET", "ais_dataset_us")
table_positions = f"{project_id}.{dataset}.ships_positions"
table_collisions = f"{project_id}.{dataset}.ships_collisions"
table_static = f"{project_id}.{dataset}.ships_static"
    
# Define tables with schema
tables_to_create = [
    {
        'table_id': table_positions,
        'schema': {
            "fields": [
                {"name": "mmsi", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "ship_name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "latitude", "type": "FLOAT", "mode": "REQUIRED"},
                {"name": "longitude", "type": "FLOAT", "mode": "REQUIRED"},
                {"name": "cog", "type": "FLOAT", "mode": "REQUIRED"},
                {"name": "sog", "type": "FLOAT", "mode": "REQUIRED"},
                {"name": "heading", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}
            ]
        },
        'time_partitioning': {
            "type_": "DAY",
            "field": "timestamp",
            "expiration_ms": 1500000  # 25 minutes
        },
        'clustering_fields': ["mmsi"]
    },
    {
        'table_id': table_collisions,
        'schema': {
            "fields": [
                {"name": "mmsi_a", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "ship_name_a", "type": "STRING", "mode": "NULLABLE"},
                {"name": "mmsi_b", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "ship_name_b", "type": "STRING", "mode": "NULLABLE"},
                {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
                {"name": "cpa", "type": "FLOAT", "mode": "REQUIRED"},
                {"name": "tcpa", "type": "FLOAT", "mode": "REQUIRED"},
                {"name": "distance", "type": "FLOAT", "mode": "REQUIRED"},
                {"name": "is_active", "type": "BOOL", "mode": "REQUIRED"},
                {"name": "latitude_a", "type": "FLOAT", "mode": "REQUIRED"},
                {"name": "longitude_a", "type": "FLOAT", "mode": "REQUIRED"},
                {"name": "latitude_b", "type": "FLOAT", "mode": "REQUIRED"},
                {"name": "longitude_b", "type": "FLOAT", "mode": "REQUIRED"}
            ]
        },
        'time_partitioning': {
            "type_": "DAY",
            "field": "timestamp",
            "expiration_ms": 600000  # 10 minutes
        },
        'clustering_fields': ["mmsi_a", "mmsi_b"]
    },
    {
        'table_id': table_static,
        'schema': {
            "fields": [
                {"name": "mmsi", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "ship_name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "dim_a", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "dim_b", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "dim_c", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "dim_d", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "update_time", "type": "TIMESTAMP", "mode": "REQUIRED"}
            ]
        },
        'time_partitioning': None,
        'clustering_fields': ["mmsi"]
    }
]

def create_bigquery_tables():
    """Creates BigQuery tables if they don't exist."""
    client = bigquery.Client()
    
    for table_info in tables_to_create:
        table_id = table_info['table_id']
        schema = table_info['schema']['fields']
        time_partitioning = table_info.get('time_partitioning')
        clustering_fields = table_info.get('clustering_fields')

        table = bigquery.Table(table_id, schema=schema)

        # Handle time partitioning
        if time_partitioning:
            time_partitioning = {k if k != 'type' else 'type_': v for k, v in time_partitioning.items()}
            table.time_partitioning = bigquery.TimePartitioning(**time_partitioning)

        # Handle clustering
        if clustering_fields:
            table.clustering_fields = clustering_fields

        try:
            client.get_table(table_id)
            logging.info(f"✅ Table {table_id} already exists.")
        except NotFound:
            try:
                client.create_table(table)
                logging.info(f"Created table: {table_id}")
            except Exception as e:
                logging.error(f"Error creating table {table_id}: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_bigquery_tables()
