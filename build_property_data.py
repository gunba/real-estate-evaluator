import json
import math
import time
from shapely.geometry import Point
from concurrent.futures import ThreadPoolExecutor

# Constants
PERTH_CBD_COORDS = (115.8617, -31.9514)
PERTH_AIRPORT_COORDS = (115.9672, -31.9385)
LOCAL_COMMUNITY_RADIUS = 1  # in kilometers
EARTH_RADIUS = 6371.0  # in kilometers

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
    # Extract the suburb from the address field
    address_parts = property_data['address'].split(', ')
    property_data['suburb'] = address_parts[-1]

    property_point = Point(property_data['longitude'], property_data['latitude'])

    # Calculate the local community population and dwelling count
    local_community_population = 0
    local_community_dwellings = 0
    for feature in mesh_block_data['features']:
        mesh_block_point = Point(feature['geometry']['coordinates'])
        distance = haversine_distance(property_point.x, property_point.y, mesh_block_point.x, mesh_block_point.y)
        if distance <= LOCAL_COMMUNITY_RADIUS:
            local_community_population += feature['properties']['Population']
            local_community_dwellings += feature['properties']['Dwelling']

    property_data['local_community_population'] = local_community_population
    property_data['local_community_dwellings'] = local_community_dwellings

    # Calculate distances and counts for OSM node features
    osm_features = {
        'nearest_fuel_station': float('inf'),
        'nearest_bus_stop': float('inf'),
        'nearest_train_station': float('inf'),
        'nearest_police_station': float('inf'),
        'nearest_healthcare_facility': float('inf'),
        'nearest_doctor_office': float('inf'),
        'nearest_dental_office': float('inf'),
        'nearest_primary_education': float('inf'),
        'nearest_higher_education': float('inf'),
        'nearest_library': float('inf'),
        'nearest_fire_station': float('inf'),
        'nearest_post_office': float('inf'),
        'nearest_community_center': float('inf'),
        'nearest_administrative_building': float('inf'),
        'nearest_financial_services': float('inf'),
        'nearest_religious_building': float('inf'),
        'local_dining_count': 0,
        'local_shop_count': 0,
        'local_waste_facility_count': 0,
        'local_tourism_count': 0,
        'local_sports_facility_count': 0,
        'local_leisure_facility_count': 0,
        'local_public_art_count': 0,
        'local_swimming_pool_count': 0,
        'local_garden_count': 0,
        'local_social_facility_count': 0
    }

    for feature in osm_node_data['features']:
        osm_point = Point(feature['geometry']['coordinates'])
        distance = haversine_distance(property_point.x, property_point.y, osm_point.x, osm_point.y)

        if 'fuel_station' in feature['properties'] and distance < osm_features['nearest_fuel_station']:
            osm_features['nearest_fuel_station'] = distance
        if 'bus_stop' in feature['properties'] and distance < osm_features['nearest_bus_stop']:
            osm_features['nearest_bus_stop'] = distance
        if 'train_station' in feature['properties'] and distance < osm_features['nearest_train_station']:
            osm_features['nearest_train_station'] = distance
        if 'police_station' in feature['properties'] and distance < osm_features['nearest_police_station']:
            osm_features['nearest_police_station'] = distance
        if 'healthcare_facility' in feature['properties'] and distance < osm_features['nearest_healthcare_facility']:
            osm_features['nearest_healthcare_facility'] = distance
        if 'doctor_office' in feature['properties'] and distance < osm_features['nearest_doctor_office']:
            osm_features['nearest_doctor_office'] = distance
        if 'dental_office' in feature['properties'] and distance < osm_features['nearest_dental_office']:
            osm_features['nearest_dental_office'] = distance
        if 'primary_education' in feature['properties'] and distance < osm_features['nearest_primary_education']:
            osm_features['nearest_primary_education'] = distance
        if 'higher_education' in feature['properties'] and distance < osm_features['nearest_higher_education']:
            osm_features['nearest_higher_education'] = distance
        if 'library' in feature['properties'] and distance < osm_features['nearest_library']:
            osm_features['nearest_library'] = distance
        if 'fire_station' in feature['properties'] and distance < osm_features['nearest_fire_station']:
            osm_features['nearest_fire_station'] = distance
        if 'post_office' in feature['properties'] and distance < osm_features['nearest_post_office']:
            osm_features['nearest_post_office'] = distance
        if 'community_center' in feature['properties'] and distance < osm_features['nearest_community_center']:
            osm_features['nearest_community_center'] = distance
        if 'administrative_building' in feature['properties'] and distance < osm_features['nearest_administrative_building']:
            osm_features['nearest_administrative_building'] = distance
        if 'financial_services' in feature['properties'] and distance < osm_features['nearest_financial_services']:
            osm_features['nearest_financial_services'] = distance
        if 'religious_building' in feature['properties'] and distance < osm_features['nearest_religious_building']:
            osm_features['nearest_religious_building'] = distance

        if distance <= LOCAL_COMMUNITY_RADIUS:
            if 'dining' in feature['properties']:
                osm_features['local_dining_count'] += 1
            if 'shop' in feature['properties']:
                osm_features['local_shop_count'] += 1
            if 'waste_facility' in feature['properties']:
                osm_features['local_waste_facility_count'] += 1
            if 'tourism' in feature['properties']:
                osm_features['local_tourism_count'] += 1
            if 'sports_facility' in feature['properties']:
                osm_features['local_sports_facility_count'] += 1
            if 'leisure_facility' in feature['properties']:
                osm_features['local_leisure_facility_count'] += 1
            if 'public_art' in feature['properties']:
                osm_features['local_public_art_count'] += 1
            if 'swimming_pool' in feature['properties']:
                osm_features['local_swimming_pool_count'] += 1
            if 'garden' in feature['properties']:
                osm_features['local_garden_count'] += 1
            if 'social_facility' in feature['properties']:
                osm_features['local_social_facility_count'] += 1

    # Calculate distances to Perth CBD and airport
    property_data['distance_to_perth_cbd'] = haversine_distance(property_point.x, property_point.y, PERTH_CBD_COORDS[0], PERTH_CBD_COORDS[1])
    property_data['distance_to_perth_airport'] = haversine_distance(property_point.x, property_point.y, PERTH_AIRPORT_COORDS[0], PERTH_AIRPORT_COORDS[1])

    # Merge the OSM feature data into the property data
    property_data.update(osm_features)

    return property_data

# Load the property data from reiwa/reiwa_listings.json
with open('reiwa/reiwa_listings.json', 'r') as file:
    property_data_list = json.load(file)

# Load the mesh block data
with open('mesh/aus_mesh_blocks_processed.geojson', 'r') as file:
    mesh_block_data = json.load(file)

# Load the OSM node data
with open('osm/osm_nodes_processed.geojson', 'r') as file:
    osm_node_data = json.load(file)

# Process a subset of properties (1000 records) and measure execution time
start_time = time.time()
subset_data_list = property_data_list[:10]

with ThreadPoolExecutor() as executor:
    results = list(executor.map(process_property, subset_data_list))

end_time = time.time()
execution_time = end_time - start_time

print(f"Processed {len(subset_data_list)} properties in {execution_time:.2f} seconds.")

# Save the updated property data to a new JSON file
with open('property_data_enriched.json', 'w') as file:
    json.dump(results, file, indent=2)

print("Property data updated with additional information and saved to 'property_data_enriched.json'.")