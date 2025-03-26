import asyncio
import websockets
import json
from dotenv import load_dotenv
import os
from datetime import datetime, timezone
from google.cloud import pubsub_v1
from google.cloud import bigquery
load_dotenv()
from google.api_core.exceptions import NotFound
from create_bq_tables import create_bigquery_tables
import time

credentials_path = './client-subscription-credentials.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
SHIPS_POSITION_TOPIC=os.getenv('SHIPS_POSITION_TOPIC')
SHIPS_STATIC_TOPIC=os.getenv('SHIPS_STATIC_TOPIC')
AIS_DATA_TOPIC=os.getenv('AIS_DATA_TOPIC')

publisher = pubsub_v1.PublisherClient()
client = bigquery.Client()

ship_static_data = {}

BUFFER_SIZE = 50
FLUSH_INTERVAL = 10

bufferShipsPositions = []
bufferShipsStatics = []
buffer_lock = asyncio.Lock()

# Buffer settings
MESSAGE_BATCH_SIZE = 10
BATCH_TIME_LIMIT = 5
message_buffer = []
last_publish_time = time.time()

async def publish_batch():
    global message_buffer, last_publish_time
    if not message_buffer:
        return
    
    print(f"Publishing {len(message_buffer)} messages...")
    
    # Publish all messages at once
    futures = []
    for message in message_buffer:
        futures.append(publisher.publish(AIS_DATA_TOPIC, message))
    
    # Wait for all messages to be published
    await asyncio.gather(*[asyncio.to_thread(future.result) for future in futures])

    # Clear buffer
    message_buffer = []
    last_publish_time = time.time()


def to_float(val):
    return float(val) if isinstance(val, (int, float)) else None

async def bulk_insert_positions():
    global bufferShipsPositions
    async with buffer_lock:
        if not bufferShipsPositions:
            return
        
        rows_to_insert = bufferShipsPositions.copy()
        bufferShipsPositions = []

    table_id = 'ais-collision-detection.ais_dataset_us.ships_positions'

    try:
        client.get_table(table_id)
    except NotFound:
        create_bigquery_tables()
        
    try:
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            print(f"BigQuery Insert Errors: {errors}")
        else:
            print(f"Successfully inserted {len(rows_to_insert)} rows into BigQuery.")
    except Exception as e:
        print(f"Bulk insert failed: {e}")

async def bulk_insert_statics():
    global bufferShipsStatics
    async with buffer_lock:
        if not bufferShipsStatics:
            return
        
        rows_to_insert = bufferShipsStatics.copy()
        bufferShipsStatics = []

    table_id = 'ais-collision-detection.ais_dataset_us.ships_static'

    try:
        client.get_table(table_id)
    except NotFound:
        create_bigquery_tables()
        
    try:
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            print(f"BigQuery Insert Errors: {errors}")
        else:
            print(f"Successfully inserted {len(rows_to_insert)} rows into BigQuery.")
    except Exception as e:
        print(f"Bulk insert failed: {e}")

async def periodic_flush():
    """Periodically flush the buffer"""
    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        await bulk_insert_positions()
        await bulk_insert_statics()
                    
def write_to_bigquery(table_id, rows):
    if not rows:
        print("No data to insert.")
        return
    
    try:
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            print(f"Encountered errors while inserting rows: {errors}")
        else:
            print(f"Successfully inserted {len(rows)} rows into {table_id}")
    except Exception as e:
        print(f"Error inserting rows into BigQuery: {e}")

async def preload_ship_data():
    global ship_static_data
    client = bigquery.Client()
    table_id = "ais-collision-detection.ais_dataset_us.ships_static"

    # Check if the table exists
    try:
        client.get_table(table_id)
    except NotFound:
        create_bigquery_tables()
        
    query = f"SELECT mmsi, ship_name, dim_a, dim_b, dim_c, dim_d FROM {table_id}"
    query_job = client.query(query)
    
    results = query_job.result()
    
    for row in results:
        ship_static_data[row.mmsi] = {
            "ship_name": row.ship_name,
            "dim_a": row.dim_a,
            "dim_b": row.dim_b,
            "dim_c": row.dim_c,
            "dim_d": row.dim_d,
        }

    print(f"Preloaded {len(ship_static_data)} ship records into cache.")
          
def is_duplicate_mmsi(mmsi):
    print(f"Checking for duplicate MMSI: {mmsi}")
    client = bigquery.Client()
    table_id = "ais-collision-detection.ais_dataset_us.ships_static"

    try:
        # Check if the table exists
        client.get_table(table_id)
    except NotFound:
        print(f"Table {table_id} does not exist. Assuming no duplicates.")
        create_bigquery_tables()

    print(f"Checking for duplicate MMSI: {mmsi}")

    query = f"""
        SELECT COUNT(*) AS count
        FROM `{table_id}`
        WHERE mmsi = @mmsi
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("mmsi", "INT64", mmsi)]
    )

    query_job = client.query(query, job_config=job_config)
    results = query_job.result()

    # Extract count from the query result
    for row in results:
        return row.count > 0  # Return True if MMSI already exists, else False

    return False

async def handle_ship_position_data(message):
    metadata = message.get("MetaData", {})
    raw_timestamp = metadata.get('time_utc')
    trimmed_timestamp = raw_timestamp[:26]
    formatted_timestamp = datetime.strptime(trimmed_timestamp, "%Y-%m-%d %H:%M:%S.%f")

    ais_message = message['Message']['PositionReport']
    mmsi = metadata.get("MMSI")
    ship_name = metadata.get("ShipName")
    if ais_message['Sog'] >= 2:       
        statics = ship_static_data.get(mmsi, {})
        if not statics:
            # print(f"Static data not found for MMSI: {mmsi}")
            return
        reduced_message = {
            "mmsi": mmsi,
            "ship_name": ship_name,
            "latitude": to_float(metadata.get("latitude")),
            "longitude": to_float(metadata.get("longitude")),
            "sog": to_float(ais_message.get('Sog')),
            "cog": to_float(ais_message.get('Cog')),
            "heading": to_float(ais_message.get('Heading')) or None,
            "timestamp": formatted_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "dim_a": statics.get("dim_a"),
            "dim_b": statics.get("dim_b"),
            "dim_c": statics.get("dim_c"),
            "dim_d": statics.get("dim_d")
        }
        payload_bytes = json.dumps(reduced_message).encode("utf-8") 
        message_buffer.append(payload_bytes)

        # Check if we should publish
        if len(message_buffer) >= MESSAGE_BATCH_SIZE or (time.time() - last_publish_time) >= BATCH_TIME_LIMIT:
            await publish_batch()               
        # future = publisher.publish(AIS_DATA_TOPIC, payload_bytes)
        # print(f"Published reduced message ID: {future.result()}")
            
        # Write to BigQuery
        write_message = {
            "mmsi": int(mmsi),
            "ship_name": ship_name if ship_name else None,
            "latitude": float(metadata.get("latitude")),
            "longitude": float(metadata.get("longitude")),  
            "cog": float(ais_message.get("Cog")),
            "sog": float(ais_message.get("Sog")),
            "heading": float(ais_message.get("Heading")) if ais_message.get("Heading") is not None else None,
            "timestamp": formatted_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        async with buffer_lock:
            bufferShipsPositions.append(write_message)

        # Trigger bulk insert when buffer is full
        if len(bufferShipsPositions) >= BUFFER_SIZE:
            await bulk_insert_positions()
            
        # # Check if the table exists
        # try:
        #     client.get_table('ais-collision-detection.ais_dataset_us.ships_positions')
        # except NotFound:
        #     create_bigquery_tables()
            
        # await asyncio.to_thread(write_to_bigquery, 'ais-collision-detection.ais_dataset_us.ships_positions', [write_message]) 

async def handle_ship_static_data(message):
    metadata = message.get("MetaData", {})
    ais_message = message['Message']['ShipStaticData']
    mmsi = metadata.get("MMSI")
    ship_name = metadata.get("ShipName")
    dimension = ais_message.get("Dimension", {})
    
    # Check for duplicates before publishing
    if ship_static_data.get(mmsi) is None:
        ship_static_data[mmsi] = {
            "ship_name": ship_name,
            "dim_a": to_float(dimension.get("A")),
            "dim_b": to_float(dimension.get("B")),
            "dim_c": to_float(dimension.get("C")),
            "dim_d": to_float(dimension.get("D"))
        }
    
        # if not is_duplicate_mmsi(mmsi):
        write_message = {
            "mmsi": int(mmsi),
            "ship_name": ship_name if ship_name else None,
            "dim_a": float(dimension.get('A')),
            "dim_b": float(dimension.get('B')),
            "dim_c": float(dimension.get('C')),
            "dim_d": float(dimension.get('D')),
            "update_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        }
        
        async with buffer_lock:
            bufferShipsStatics.append(write_message)

        # Trigger bulk insert when buffer is full
        if len(bufferShipsStatics) >= BUFFER_SIZE:
            await bulk_insert_positions()
        
        # Check if the table exists
        # try:
        #     client.get_table('ais-collision-detection.ais_dataset_us.ships_static')
        # except NotFound:
        #     create_bigquery_tables()
            
        # await asyncio.to_thread(write_to_bigquery, 'ais-collision-detection.ais_dataset_us.ships_static', [write_message])        
    
async def connect_ais_stream():
    while True:
        try:
            async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
                subscribe_message = {"APIKey": os.getenv("AIS_STREAM_API_KEY"),
                                    "BoundingBoxes": [
                                        # [[-90, -180], [90, 180]]
                                        [[49.0, -2.0], [51.0, 2.0]]
                                        ],
                                    "OutputFormat": "JSON",
                                    "Compression": "None",
                                    "BufferSize": 1,
                                    "FilterMessageTypes": ["PositionReport", "ShipStaticData"]}

                subscribe_message_json = json.dumps(subscribe_message)
                await websocket.send(subscribe_message_json)

                async for message_json in websocket:
                    message = json.loads(message_json)
                    message_type = message["MessageType"]

                    # print(f"[{datetime.now(timezone.utc)}] Received message of type: {message_type}")
                    if message_type == "PositionReport":
                        await handle_ship_position_data(message)
                            
                    if message_type == "ShipStaticData":
                        await handle_ship_static_data(message)
        except websockets.exceptions.ConnectionClosedError as e:
            print(f"WebSocket closed: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

        except Exception as e:
            print(f"Unexpected error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)
            
async def main():
    await preload_ship_data()
    await connect_ais_stream()
                
if __name__ == "__main__":
    asyncio.run(main())