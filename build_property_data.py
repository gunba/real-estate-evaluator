import json
import math
import time
from multiprocessing import Pool
from tqdm import tqdm

# Constants
PERTH_CBD_COORDS = (115.8617, -31.9514)
PERTH_AIRPORT_COORDS = (115.9672, -31.9385)
LOCAL_COMMUNITY_RADIUS = 1.5  # in kilometers
EARTH_RADIUS = 6371.0  # in kilometers

# Map for how to aggregate features
feature_categories = {
    'fuel_station': 'nearest',
    'bus_stop': 'nearest',
    'train_station': 'nearest',
    'police_station': 'nearest',
    'healthcare_facility': 'nearest',
    'doctor_office': 'nearest',
    'dental_office': 'nearest',
    'primary_education': 'nearest',
    'higher_education': 'nearest',
    'library': 'nearest',
    'fire_station': 'nearest',
    'post_office': 'nearest',
    'community_center': 'nearest',
    'administrative_building': 'nearest',
    'financial_services': 'nearest',
    'religious_building': 'nearest',
    'dining': 'local',
    'shop': 'local',
    'waste_facility': 'local',
    'tourism': 'local',
    'sports_facility': 'local',
    'leisure_facility': 'local',
    'public_art': 'local',
    'swimming_pool': 'local',
    'garden': 'local',
    'social_facility': 'local'
}

# Create the feature template for use inside the property data function
osm_feature_template = {}
for feature_type, category in feature_categories.items():
    if category == 'nearest':
        osm_feature_template[f'osm_nearest_{feature_type}'] = float('inf')
    elif category == 'local':
        osm_feature_template[f'osm_local_{feature_type}'] = 0

# Load the property data from reiwa/reiwa_listings.json
with open('reiwa/reiwa_listings.json', 'r') as file:
    property_data_list = json.load(file)

# Load the mesh block data
with open('mesh/aus_mesh_blocks_processed.geojson', 'r') as file:
    mesh_block_data = json.load(file)

# Load the OSM node data
with open('osm/osm_nodes_processed.geojson', 'r') as file:
    osm_node_data = json.load(file)

    # Load school data
with open('school_data.json') as f:
    scsa_school_data = json.load(f)

# Load the suburb data from suburb_data.json
with open('suburb_data.json', 'r') as file:
    suburb_data = json.load(file)

def haversine_distance(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the Earth (specified in decimal degrees)
    """
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.asin(math.sqrt(a))
    distance = EARTH_RADIUS * c
    return distance

def process_property(property_data):
    # Check if the suburb exists in the suburb data
    suburb = property_data['reiwa_suburb']
    if suburb not in suburb_data:
        return None

    property_lon = property_data['reiwa_longitude']
    property_lat = property_data['reiwa_latitude']

    min_distance = float('inf')
    closest_school = None
    for school in scsa_school_data:
        school_lon = school['longitude']
        school_lat = school['latitude']
        distance = haversine_distance(property_lon, property_lat, school_lon, school_lat)
        if distance < min_distance:
            min_distance = distance
            closest_school = school

    # Calculate the local community population and dwelling count
    local_community_population = 0
    local_community_dwellings = 0
    for feature in mesh_block_data['features']:
        mesh_block_lon = feature['geometry']['coordinates'][0]
        mesh_block_lat = feature['geometry']['coordinates'][1]
        distance = haversine_distance(property_lon, property_lat, mesh_block_lon, mesh_block_lat)
        if distance <= LOCAL_COMMUNITY_RADIUS:
            local_community_population += feature['properties']['Population']
            local_community_dwellings += feature['properties']['Dwelling']

    property_data['osm_local_community_population'] = local_community_population
    property_data['osm_local_community_dwellings'] = local_community_dwellings

    # Calculate haversine distances for each OSM node feature and store them in osm_node_indices
    osm_node_indices = []
    for idx, feature in enumerate(osm_node_data['features']):
        osm_lon = feature['geometry']['coordinates'][0]
        osm_lat = feature['geometry']['coordinates'][1]
        distance = haversine_distance(property_lon, property_lat, osm_lon, osm_lat)
        osm_node_indices.append((idx, distance))

    # Sort the indices of OSM node features by distance to the property
    osm_node_indices.sort(key=lambda x: x[1])

    # Create copy of feature template for storing data
    osm_features = osm_feature_template.copy()

    # Once we have found values for all nearest_ keys, we can exit once we exceed local distance
    all_nearest_found = False

    for idx, distance in osm_node_indices:
        feature = osm_node_data['features'][idx]
        properties = feature['properties']

        for feature_type, category in feature_categories.items():
            if feature_type in properties:
                if category == 'nearest' and distance < osm_features[f'osm_nearest_{feature_type}']:
                    osm_features[f'osm_nearest_{feature_type}'] = distance
                    break
                elif category == 'local' and distance <= LOCAL_COMMUNITY_RADIUS:
                    osm_features[f'osm_local_{feature_type}'] += 1

        all_nearest_found = all(value != float('inf') for key, value in osm_features.items() if key.startswith('osm_nearest_'))

        if all_nearest_found and distance > LOCAL_COMMUNITY_RADIUS:
            break

    # Calculate distances to Perth CBD and airport
    property_data['osm_distance_to_perth_cbd'] = haversine_distance(property_lon, property_lat, PERTH_CBD_COORDS[0], PERTH_CBD_COORDS[1])
    property_data['osm_distance_to_perth_airport'] = haversine_distance(property_lon, property_lat, PERTH_AIRPORT_COORDS[0], PERTH_AIRPORT_COORDS[1])

    # Merge the OSM feature data into the property data
    property_data.update(osm_features)

    # Merge the suburb data into the property data
    property_data.update(suburb_data[suburb])

    # Merge school data into property data
    property_data.update(closest_school["achievement_data"])
    return property_data

if __name__ == '__main__':
    # Process properties using multiprocessing and measure execution time
    start_time = time.time()

    # Track unique reiwa_listing_id values
    unique_listing_ids = set()
    unique_property_data_list = []

    for property_data in property_data_list:
        reiwa_listing_id = property_data['reiwa_listing_id']
        if reiwa_listing_id not in unique_listing_ids:
            unique_listing_ids.add(reiwa_listing_id)
            unique_property_data_list.append(property_data)

    with Pool() as pool:
        results = []
        with tqdm(total=len(unique_property_data_list), unit='property', desc='Processing properties') as pbar:
            for i, _ in enumerate(pool.imap_unordered(process_property, unique_property_data_list), start=1):
                results.append(_)
                pbar.update()

    updated_property_data_list = [property for property in results if property is not None]

    end_time = time.time()
    execution_time = end_time - start_time

    print(f"\nProcessed {len(unique_property_data_list)} properties in {execution_time:.2f} seconds.")

    # Save the updated property data to a new JSON file
    with open('property_data.json', 'w') as file:
        json.dump(updated_property_data_list, file, indent=2)

    print("Property data updated with additional information and saved to 'property_data.json'.")
