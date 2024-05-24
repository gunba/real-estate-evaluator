import json
from shapely.geometry import shape
import math

def add_area_to_suburbs(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        geojson_data = json.load(file)

    # Earth's radius in kilometers (approximate)
    earth_radius = 6371

    filtered_features = []

    for feature in geojson_data['features']:
        if feature['geometry']['type'] == 'Polygon' and 'name' in feature['properties']:
            suburb_geometry = shape(feature['geometry'])
            suburb_area_sq_degrees = suburb_geometry.area

            # Convert area from square degrees to square kilometers
            suburb_area_km2 = suburb_area_sq_degrees * (math.pi / 180) ** 2 * earth_radius ** 2

            feature['properties']['area_km2'] = suburb_area_km2
            filtered_features.append(feature)

    geojson_data['features'] = filtered_features
    return geojson_data

def save_geojson_data(geojson_data, file_path):
    with open(file_path, 'w') as file:
        json.dump(geojson_data, file, indent=2)

if __name__ == '__main__':
    input_file_path = 'osm_suburbs.geojson'
    output_file_path = 'osm_suburbs_processed.geojson'

    updated_geojson_data = add_area_to_suburbs(input_file_path)
    save_geojson_data(updated_geojson_data, output_file_path)
