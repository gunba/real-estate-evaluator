import json
import csv

# Load the exported GeoJSON data
with open("aus_mesh_blocks.geojson") as file:
    geojson_data = json.load(file)

# Create a dictionary to store the mesh block data by MB_CODE_21
mesh_block_dict = {feature["properties"]["MB_CODE21"]: feature for feature in geojson_data["features"]}

# Convert geojson polygon collection into single point (for calculating distance later)
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

# Create a new GeoJSON feature collection
new_features = []

# Read the mesh_block_census.csv file and process each entry
with open("mesh_block_census.csv") as file:
    csv_reader = csv.reader(file)
    next(csv_reader)  # Skip the header row
    for row in csv_reader:
        try:
            mb_code = row[0]
            dwelling = int(row[3])
            population = int(row[4])
            
            if mb_code in mesh_block_dict:
                feature = mesh_block_dict[mb_code]
                coordinates = feature["geometry"]["coordinates"][0]
                centroid = average_bounding_box(coordinates)
                
                new_feature = {
                    "type": "Feature",
                    "properties": {
                        "MB_CODE_21": mb_code,
                        "Dwelling": dwelling,
                        "Population": population
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": centroid
                    }
                }
                new_features.append(new_feature)
        except Exception as e: 
            print(e)

# Create the new GeoJSON object
new_geojson = {
    "type": "FeatureCollection",
    "name": "aus_mesh_blocks_census",
    "crs": geojson_data["crs"],
    "features": new_features
}

# Save the new GeoJSON data to a file
with open("aus_mesh_blocks_census.geojson", "w") as file:
    json.dump(new_geojson, file)