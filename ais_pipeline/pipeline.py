import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import json
import geohash
from math import radians, cos, sin, sqrt, atan2
import os 
from dotenv import load_dotenv
load_dotenv()
from apache_beam import window
import math
from datetime import datetime

credentials_path = './client-subscription-credentials.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

class ParsePubSubMessage(beam.DoFn):
    def process(self, element):
        try:
            message = json.loads(element.decode("utf-8"))
            required_fields = ["mmsi", "latitude", "longitude", "sog", "cog", "timestamp"]
                        
            if all(field in message for field in required_fields):
                yield message
        except Exception as e:
            print(f"Error parsing message: {e}")

class FormatForBigQuery(beam.DoFn):
    def process(self, element):
        ship_a, ship_b, cpa, tcpa, distance = element

        formatted_row = {
            "mmsi_a": ship_a.get("mmsi", ""),
            "ship_name_a": ship_a.get("ship_name", "").strip(),
            "mmsi_b": ship_b.get("mmsi", ""),
            "ship_name_b": ship_b.get("ship_name", "").strip(),
            "cpa": float(cpa),
            "tcpa": float(tcpa),
            "distance": float(distance),
            "is_active": True,
            "latitude_a": float(ship_a.get("latitude", 0.0)),
            "longitude_a": float(ship_a.get("longitude", 0.0)),
            "latitude_b": float(ship_b.get("latitude", 0.0)),
            "longitude_b": float(ship_b.get("longitude", 0.0)),
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        }
        yield formatted_row
        
def assign_geohash(ship):
    print("Assigning geohash")
    precision = 3
    ship["geohash"] = geohash.encode(ship["latitude"], ship["longitude"], precision)
    print(f"Assigned geohash: {ship['geohash']}")
    return ship

def haversine(lat1, lon1, lat2, lon2):
    R = 3440.065
    # R = 6371.0  # Radius of Earth in km
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c 

def haversine_nm(lat1, lon1, lat2, lon2):
    R_NM = 3440.065
    rad = math.pi / 180  # Conversion factor for degrees to radians

    dLat = (lat2 - lat1) * rad
    dLon = (lon2 - lon1) * rad

    a = math.sin(dLat / 2) ** 2 + math.cos(lat1 * rad) * math.cos(lat2 * rad) * math.sin(dLon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R_NM * c

# Identify ships within 5 nautical miles
def find_nearby_ships(grouped_ships):
    print("Finding nearby ships")
    geohash, ships = grouped_ships
    print(f"Processing geohash: {geohash}, Number of ships: {len(ships)}")
    
    if len(ships) < 2:
        print(f"Skipping geohash {geohash} (not enough ships)")
        return []  # No possible collisions

    nearby_pairs = []
    
    for i, ship_a in enumerate(ships):
        for j, ship_b in enumerate(ships):
            if i >= j:
                continue
            
            dist = haversine_nm(ship_a["latitude"], ship_a["longitude"],
                             ship_b["latitude"], ship_b["longitude"])
            
            if dist <= 15:  # Within 5 nautical miles
                approach_angle = compute_dot_product(ship_a, ship_b)
                result = (ship_a, ship_b, dist, approach_angle)
                print(f"Geohash {geohash}: Ship {ship_a['mmsi']} close to {ship_b['mmsi']} at {dist} Nm")
                nearby_pairs.append(result)

    if not nearby_pairs:
        print(f"Geohash {geohash}: No nearby ships found")  # Debugging
    return nearby_pairs

def get_heading_or_cog(ship):
    return ship.get("heading") if ship.get("heading") is not None else ship.get("cog", 0)

def to_xy(lat, lon):
    x = lon * 60 * math.cos(math.radians(lat))
    y = lat * 60
    return x, y

def cog_to_vector(cog, sog):
    rad = math.radians(cog)
    vx = sog * math.sin(rad)
    vy = sog * math.cos(rad)
    return vx, vy

# Compute dot product to check approach direction
def compute_dot_product(ship_a, ship_b):
    print(f"Computing dot product")
    vx1, vy1 = math.cos(math.radians(get_heading_or_cog(ship_a))), math.sin(math.radians(get_heading_or_cog(ship_a)))
    vx2, vy2 = math.cos(math.radians(get_heading_or_cog(ship_b))), math.sin(math.radians(get_heading_or_cog(ship_b)))

    print("dot product: ", vx1 * vx2 + vy1 * vy2)
    return vx1 * vx2 + vy1 * vy2  # Returns a value between -1 (head-on) and 1 (same direction)

# Calculate CPA and TCPA

def to_xy(lat, lon):
    """Convert latitude and longitude to Cartesian coordinates (Flat Earth approximation)."""
    return (lat, lon)  # This function should be replaced with the correct transformation

def cog_to_vector(cog, sog):
    """Convert course over ground (COG) and speed over ground (SOG) to velocity components."""
    rad = math.radians(cog)
    return (sog * math.sin(rad), sog * math.cos(rad))

def calculate_cpa_tcpa(ship_pair):
    shipA, shipB, distance, approach_angle = ship_pair

    # Convert positions to Cartesian coordinates
    xA, yA = to_xy(shipA['latitude'], shipA['longitude'])
    xB, yB = to_xy(shipB['latitude'], shipB['longitude'])
    dx, dy = xA - xB, yA - yB

    # Convert COG & SOG to velocity vectors
    vxA, vyA = cog_to_vector(shipA['cog'], shipA['sog'])
    vxB, vyB = cog_to_vector(shipB['cog'], shipB['sog'])
    dvx, dvy = vxA - vxB, vyA - vyB

    # Calculate TCPA
    v2 = dvx**2 + dvy**2
    tcpa_hours = 0
    if v2 != 0:
        tcpa_hours = - (dx * dvx + dy * dvy) / v2
        if tcpa_hours < 0:
            tcpa_hours = 0  # No future collision risk

    # Convert TCPA to minutes
    tcpa_minutes = tcpa_hours * 60

    # Compute CPA positions
    xA_cpa, yA_cpa = xA + vxA * tcpa_hours, yA + vyA * tcpa_hours
    xB_cpa, yB_cpa = xB + vxB * tcpa_hours, yB + vyB * tcpa_hours

    # Compute CPA distance (NM)
    cpa = math.sqrt((xA_cpa - xB_cpa)**2 + (yA_cpa - yB_cpa)**2)

    return (shipA, shipB, cpa, tcpa_minutes, distance)

# Filter and store collision pairs
def filter_collision_risks(collision_data):
    print(f"Received collision data: {collision_data}")
    ship_a, ship_b, cpa, tcpa, distance = collision_data
    if cpa < 0.5 and 0 < tcpa < 10:
        print(f"High-risk collision detected: {ship_a['mmsi']} and {ship_b['mmsi']} at {cpa} Nm")
        print(f"TCPA: {tcpa} minutes")
        print("Writing to BigQuery")
        print("------------Ending-----------------")
        return collision_data        
    return None

def is_ship_long_enough(ship):  
    print("------------Starting-----------------")
    mmsi = ship.get("mmsi")
    if not mmsi:
        print("Invalid MMSI provided.")
        return False
    
    dim_a = ship.get("dim_a")
    dim_b = ship.get("dim_b")

    # Ensure both values are not None before performing addition
    if dim_a is None or dim_b is None:
        # print(f"Skipping ship {ship.get('mmsi')} due to missing dimensions: dim_a={dim_a}, dim_b={dim_b}")
        print("ship is not long enough")
        return False

    if (dim_a + dim_b) > 50:
        print("ship is long enough")
        return True
    
    print("ship is not long enough")
    return False     

# Beam Pipeline
def run_pipeline():
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "ais-collision-detection")
    dataset = os.getenv("LIVE_DATASET", "ais_dataset_us")
    region = os.getenv("REGION", "us-east1")
    temp_loc = os.getenv("TEMP_LOCATION", "gs://ais-collision-detection-bucket/temp")
    staging_loc = os.getenv("STAGING_LOCATION", "gs://ais-collision-detection-bucket/staging")
    job_name = os.getenv("JOB_NAME", "ais-collision-detection-job")
    table_positions = f"{project_id}.{dataset}.ships_positions"
    table_collisions = f"{project_id}.{dataset}.ships_collisions"
    table_static = f"{project_id}.{dataset}.ships_static"
    
    pipeline_options = PipelineOptions(
        runner="DirectRunner",
        # project=project_id,
        # region=region,
        # temp_location=temp_loc,
        # staging_location=staging_loc,
        # job_name=os.getenv("JOB_NAME", "ais-collision-detection-job"),
        # autoscaling_algorithm='THROUGHPUT_BASED',
        # save_main_session=True,
        streaming=True,
    )
    # pipeline_options.view_as(SetupOptions).setup_file = "./setup.py"
 
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=os.getenv('AIS_DATA_SUBSCRIPTION'))
            | "Parse JSON" >> beam.ParDo(ParsePubSubMessage())
            | "Filter Short Ships" >> beam.Filter(is_ship_long_enough)
            | "Assign Geohash" >> beam.Map(assign_geohash)
            | "Filter Valid Geohash" >> beam.Filter(lambda r: r and "geohash" in r)
            # | "KeyByGeohash" >> beam.Map(lambda r: (r["geohash"], r))
            | "KeyByGeohash" >> beam.Map(lambda r: (r["geohash"][:3], r))  # Broader grouping
            # | "WindowForCollisions" >> beam.WindowInto(window.FixedWindows(10), allowed_lateness=Duration(0))
            | "WindowForCollisions" >> beam.WindowInto(window.FixedWindows(size=30))  # Longer time window
            # | "WindowForCollisions" >> beam.WindowInto(window.SlidingWindows(size=30, period=10))
            | "GroupByGeohash" >> beam.GroupByKey()
            | "Find Nearby Ships" >> beam.FlatMap(find_nearby_ships)
            | "Calculate CPA/TCPA" >> beam.Map(calculate_cpa_tcpa)
            | "Filter High-Risk Collisions" >> beam.Filter(filter_collision_risks)
            | "Format for BigQuery" >> beam.ParDo(FormatForBigQuery())         
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table="ais-collision-detection.ais_dataset_us.ships_collisions",
                schema="""
                    mmsi_a:INTEGER,
                    ship_name_a:STRING,
                    mmsi_b:INTEGER,
                    ship_name_b:STRING,
                    cpa:FLOAT,
                    tcpa:FLOAT,
                    distance:FLOAT,
                    is_active:BOOL,
                    latitude_a:FLOAT,
                    longitude_a:FLOAT,
                    latitude_b:FLOAT,
                    longitude_b:FLOAT,
                    timestamp:TIMESTAMP
                    """,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                custom_gcs_temp_location="gs://temp_bucket_pubsubtobq/temp",
                method="STREAMING_INSERTS",
            )          
        )

if __name__ == "__main__":
    run_pipeline()