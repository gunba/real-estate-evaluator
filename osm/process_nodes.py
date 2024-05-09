import json
import os

def average_bounding_box(coordinates):
    x_sum = 0
    y_sum = 0
    area = 0
    for i in range(len(coordinates[0])):
        x1, y1 = coordinates[0][i]
        x2, y2 = coordinates[0][(i + 1) % len(coordinates[0])]
        cross_product = x1 * y2 - x2 * y1
        area += cross_product
        x_sum += (x1 + x2) * cross_product
        y_sum += (y1 + y2) * cross_product
    area *= 0.5
    centroid_x = x_sum / (6 * area)
    centroid_y = y_sum / (6 * area)
    return [centroid_x, centroid_y]

def process_geojson_data(file_path):
    with open(file_path, 'r', errors="ignore") as file:
        geojson_data = json.load(file)

    property_combinations = {}

    for feature in geojson_data['features']:
        if feature['geometry']['type'] == 'Polygon':
            avg_point = average_bounding_box(feature['geometry']['coordinates'])
            feature['geometry'] = {
                'type': 'Point',
                'coordinates': avg_point
            }

        properties = feature['properties']
        for key, value in properties.items():
            combination = f"{key},{value}"
            if combination in property_combinations:
                property_combinations[combination] += 1
            else:
                property_combinations[combination] = 1

    with open('property_combinations.txt', 'w') as file:
        sorted_combinations = sorted(property_combinations.items(), key=lambda x: x[1], reverse=True)
        for combination, frequency in sorted_combinations:
            file.write(f"{combination}: {frequency}\n")

    with open('osm_nodes_processed.geojson', 'w') as file:
        json.dump(geojson_data, file, indent=2)

if __name__ == '__main__':
    file_path = 'osm_nodes.geojson'
    process_geojson_data(file_path)