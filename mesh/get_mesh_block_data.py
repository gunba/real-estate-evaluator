import json
import csv
import requests
import zipfile
import io
import os
import shapefile  # pyshp library
import pandas as pd
from geojson import Feature, FeatureCollection

# URLs for the required files
census_csv_url = "https://www.abs.gov.au/census/guide-census-data/mesh-block-counts/2021/Mesh%20Block%20Counts%2C%202021.xlsx"
geojson_url = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/MB_2021_AUST_SHP_GDA2020.zip"

# Directory to store extracted shapefile components
shapefile_dir = "shapefile_data"
use_local_files = True  # Toggle this to switch between downloading and using local files

if not use_local_files:
    # Download and process the census Excel file
    response = requests.get(census_csv_url)
    xlsx_data = io.BytesIO(response.content)
    df = pd.read_excel(xlsx_data, sheet_name='Table 5', skiprows=7, usecols='A:E', nrows=None)

    # Remove rows with all NaN values
    df.dropna(how='all', inplace=True)

    # Save to CSV
    df.to_csv('mesh_block_census.csv', index=False)

    # Download and extract the shapefile zip
    response = requests.get(geojson_url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        z.extractall(shapefile_dir)
else:
    print("Using local files.")

# Debug: Print the current working directory and the contents of the extraction directory
print("Current working directory:", os.getcwd())
print("Contents of extraction directory:")
print(os.listdir(shapefile_dir))

# Check if the required files are present
required_files = ["MB_2021_AUST_GDA2020.shp", "MB_2021_AUST_GDA2020.dbf", "MB_2021_AUST_GDA2020.shx"]
for file in required_files:
    file_path = os.path.join(shapefile_dir, file)
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Required file {file} not found in extracted contents at {file_path}.")

# Read the shapefile
sf = shapefile.Reader(os.path.join(shapefile_dir, "MB_2021_AUST_GDA2020.shp"))

# Create a GeoJSON feature collection
features = []
for shape_record in sf.shapeRecords():
    shape = shape_record.shape
    if shape.shapeType == shapefile.NULL:
        continue  # Skip shapes with type "NULL"
    properties = shape_record.record.as_dict()
    if not properties["MB_CODE21"].startswith('5'):
        continue  # Skip features not starting with '5' (WA state code)
    geometry = shape.__geo_interface__
    features.append(Feature(geometry=geometry, properties=properties))

# Create the filtered GeoJSON data object
geojson_data = FeatureCollection(features)

# Save the filtered GeoJSON data
with open("aus_mesh_blocks.geojson", "w") as file:
    json.dump(geojson_data, file)

# Load the filtered GeoJSON data
with open("aus_mesh_blocks.geojson") as file:
    geojson_data = json.load(file)

# Create a dictionary to store the mesh block data by MB_CODE21
mesh_block_dict = {feature["properties"]["MB_CODE21"]: feature for feature in geojson_data["features"]}

# Convert geojson polygon collection into single point (for calculating distance later)
def average_bounding_box(coordinates):
    x_sum = 0
    y_sum = 0
    area = 0
    n = len(coordinates)
    for i in range(n):
        x1, y1 = coordinates[i]
        x2, y2 = coordinates[(i + 1) % n]
        cross_product = x1 * y2 - x2 * y1
        area += cross_product
        x_sum += (x1 + x2) * cross_product
        y_sum += (y1 + y2) * cross_product
    area *= 0.5
    if area == 0:  # In case of degenerate polygons
        centroid_x = sum(x for x, y in coordinates) / n
        centroid_y = sum(y for x, y in coordinates) / n
    else:
        centroid_x = x_sum / (6 * area)
        centroid_y = y_sum / (6 * area)
    return [centroid_x, centroid_y]

# Function to safely convert values to integers
def safe_int(value):
    try:
        return int(float(value))
    except ValueError:
        return None

# Create a new GeoJSON feature collection with additional census data
new_features = []

# Read the mesh_block_census.csv file and process each entry
with open("mesh_block_census.csv") as file:
    csv_reader = csv.reader(file)
    next(csv_reader)  # Skip the header row
    for row in csv_reader:
        try:
            mb_code = row[0]
            dwelling = safe_int(row[3])
            population = safe_int(row[4])
            
            if mb_code in mesh_block_dict and dwelling is not None and population is not None:
                feature = mesh_block_dict[mb_code]
                coordinates = feature["geometry"]["coordinates"]
                centroid = average_bounding_box(coordinates[0])  # Take the first set of coordinates
                
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

# Create the new GeoJSON object without crs
new_geojson = {
    "type": "FeatureCollection",
    "name": "aus_mesh_blocks_processed",
    "features": new_features
}

# Save the new GeoJSON data to a file
with open("aus_mesh_blocks_processed.geojson", "w") as file:
    json.dump(new_geojson, file)
