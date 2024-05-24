import json
import requests

USE_LOCAL_FILES = False

def fetch_overpass_data(query, output_file):
    url = "http://overpass-api.de/api/interpreter"
    response = requests.post(url, data={'data': query})
    response.raise_for_status()  # Check that the request was successful
    data = response.json()
    
    # Save the original data to a file
    with open(output_file, 'w') as file:
        json.dump(data, file, indent=2)
    
    return data

def average_coordinates(node_ids, nodes):
    coordinates = [nodes[node_id][:2] for node_id in node_ids if node_id in nodes]
    if not coordinates:
        return None
    lon = sum(coord[0] for coord in coordinates) / len(coordinates)
    lat = sum(coord[1] for coord in coordinates) / len(coordinates)
    return [lon, lat]

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

def convert_to_geojson(osm_data):
    geojson_data = {
        "type": "FeatureCollection",
        "features": []
    }
    
    nodes = {element['id']: (element['lon'], element['lat'], element.get('tags', {})) for element in reversed(osm_data['elements']) if element['type'] == 'node'}
    merged_nodes = set()  # Set to keep track of merged nodes

    for element in osm_data['elements']:
        if element['type'] == 'way':
            node_ids = set(element['nodes'])
            avg_coordinates = average_coordinates(node_ids, nodes)
            if avg_coordinates:
                combined_tags = element.get("tags", {}).copy()
                
                for node_id in node_ids:
                    if node_id in nodes:
                        node_tags = nodes[node_id][2]
                        for key, value in node_tags.items():
                            if key not in combined_tags:
                                combined_tags[key] = value
                        merged_nodes.add(node_id)  # Track merged nodes
                
                feature = {
                    "type": "Feature",
                    "properties": combined_tags,
                    "geometry": {
                        "type": "Point",
                        "coordinates": avg_coordinates
                    }
                }
                geojson_data["features"].append(feature)
    
    for element in osm_data['elements']:
        if element['type'] == 'node' and element['id'] not in merged_nodes:  # Skip merged nodes
            feature = {
                "type": "Feature",
                "properties": element.get("tags", {}),
                "geometry": {
                    "type": "Point",
                    "coordinates": [element["lon"], element["lat"]]
                }
            }
            geojson_data["features"].append(feature)
    
    return geojson_data

def simplify_geojson(geojson_data):
    simplified_features = []
    for feature in geojson_data["features"]:
        properties = feature["properties"]
        geometry = feature["geometry"]
        simplified_properties = {"name": properties.get("name", "")}
        
        for key, value in properties.items():
            if key == "coast" and value == 1:
                simplified_properties["coast"] = 1
            elif key == "amenity":
                if value in ["cafe", "fast_food", "restaurant", "pub", "bar", "biergarten", "food_court", "ice_cream"]:
                    simplified_properties["dining"] = 1
                elif value in ["parking", "parking_entrance", "parking_space", "motorcycle_parking", "bicycle_parking"]:
                    simplified_properties["parking"] = 1
                elif value in ["bench", "shelter", "lounger", "shower", "toilets", "drinking_water"]:
                    simplified_properties["public_amenities"] = 1
                elif value in ["hospital", "clinic"]:
                    simplified_properties["healthcare_facility"] = 1
                elif value in ["doctors"]:
                    simplified_properties["doctor_office"] = 1
                elif value in ["dentist"]:
                    simplified_properties["dental_office"] = 1
                elif value in ["school", "kindergarten"]:
                    simplified_properties["primary_education"] = 1
                elif value in ["college", "university"]:
                    simplified_properties["higher_education"] = 1
                elif value in ["library"]:
                    simplified_properties["library"] = 1
                elif value in ["police"]:
                    simplified_properties["police_station"] = 1
                elif value in ["fire_station"]:
                    simplified_properties["fire_station"] = 1
                elif value in ["post_office"]:
                    simplified_properties["post_office"] = 1
                elif value in ["community_centre"]:
                    simplified_properties["community_center"] = 1
                elif value in ["townhall", "courthouse"]:
                    simplified_properties["administrative_building"] = 1
                elif value in ["bank", "atm", "bureau_de_change"]:
                    simplified_properties["financial_services"] = 1
                elif value in ["place_of_worship", "monastery"]:
                    simplified_properties["religious_building"] = 1
                elif value in ["fuel"]:
                    simplified_properties["fuel_station"] = 1
                elif value in ["nightclub"]:
                    simplified_properties["nightclub"] = 1
                elif value in ["cinema", "theatre"]:
                    simplified_properties["entertainment_venue"] = 1
                elif value in ["waste_basket", "waste_disposal", "dog_excrement", "recycling", "trash"]:
                    simplified_properties["waste_facility"] = 1
                else:
                    simplified_properties["miscellaneous_amenity"] = 1
            elif key.startswith("shop"):
                simplified_properties["shop"] = 1
            elif key.startswith("tourism"):
                simplified_properties["tourism"] = 1
            elif key.startswith("sport"):
                simplified_properties["sports_facility"] = 1
            elif key.startswith("leisure"):
                simplified_properties["leisure_facility"] = 1
            elif key.startswith("artwork_type"):
                simplified_properties["public_art"] = 1
            elif key == "highway" and value == "bus_stop":
                simplified_properties["bus_stop"] = 1
            elif key == "railway" and value == "station":
                simplified_properties["train_station"] = 1
            elif key.startswith("swimming_pool"):
                simplified_properties["swimming_pool"] = 1
            elif key.startswith("garden:type"):
                simplified_properties["garden"] = 1
            elif key.startswith("social_facility"):
                simplified_properties["social_facility"] = 1
        
        if simplified_properties:
            simplified_feature = {
                "type": "Feature",
                "properties": simplified_properties,
                "geometry": geometry
            }
            simplified_features.append(simplified_feature)
    
    simplified_geojson = {
        "type": "FeatureCollection",
        "features": simplified_features
    }
    
    return simplified_geojson

def add_coast_property(coast_geojson_data):
    new_features = []
    
    for feature in coast_geojson_data['features']:
        if feature['geometry']['type'] == 'MultiLineString':
            for line in feature['geometry']['coordinates']:
                for coord in line:
                    new_feature = {
                        'type': 'Feature',
                        'geometry': {
                            'type': 'Point',
                            'coordinates': coord
                        },
                        'properties': {'coast': 1}
                    }
                    new_features.append(new_feature)
    
    coast_geojson_data['features'] = new_features
    return coast_geojson_data

def process_geojson_data(main_file_path, coast_file_path):
    with open(main_file_path, 'r', errors="ignore") as main_file:
        geojson_data = json.load(main_file)

    with open(coast_file_path, 'r', errors="ignore") as coast_file:
        coast_geojson_data = json.load(coast_file)

    coast_geojson_data = add_coast_property(coast_geojson_data)
    
    # Append coast features to main geojson features
    geojson_data['features'].extend(coast_geojson_data['features'])

    property_combinations = {}

    for feature in geojson_data['features']:
        if feature['geometry']['type'] == 'Polygon':
            avg_point = average_bounding_box(feature['geometry']['coordinates'])
            feature['geometry'] = {
                'type': 'Point',
                'coordinates': avg_point
            }
        elif feature['geometry']['type'] == 'MultiPolygon':
            avg_points = []
            for polygon in feature['geometry']['coordinates']:
                avg_point = average_bounding_box(polygon)
                avg_points.append(avg_point)
            centroid = [sum(x) / len(x) for x in zip(*avg_points)]
            feature['geometry'] = {
                'type': 'Point',
                'coordinates': centroid
            }
        elif feature['geometry']['type'] == 'LineString':
            coordinates = feature['geometry']['coordinates']
            centroid = [sum(x) / len(x) for x in zip(*coordinates)]
            feature['geometry'] = {
                'type': 'Point',
                'coordinates': centroid
            }
        properties = feature['properties']
        for key, value in properties.items():
            combination = f"{key},{value}"
            if combination in property_combinations:
                property_combinations[combination] += 1
            else:
                property_combinations[combination] = 1

    with open('property_combinations.txt', 'w', encoding='utf-8') as file:
        sorted_combinations = sorted(property_combinations.items(), key=lambda x: x[1], reverse=True)
        for combination, frequency in sorted_combinations:
            file.write(f"{combination}: {frequency}\n")

    geojson_data = simplify_geojson(geojson_data)

    with open('osm_nodes_processed.geojson', 'w') as file:
        json.dump(geojson_data, file, indent=2)

if __name__ == '__main__':
    main_query = """
    [out:json][timeout:25];
    (
      node["amenity"](-33.04320549616556,114.69451904296876,-30.767799150881462,117.78717041015625);
      way["amenity"](-33.04320549616556,114.69451904296876,-30.767799150881462,117.78717041015625);
      node["shop"](-33.04320549616556,114.69451904296876,-30.767799150881462,117.78717041015625);
      way["shop"](-33.04320549616556,114.69451904296876,-30.767799150881462,117.78717041015625);
      node["tourism"](-33.04320549616556,114.69451904296876,-30.767799150881462,117.78717041015625);
      way["tourism"](-33.04320549616556,114.69451904296876,-30.767799150881462,117.78717041015625);
      node["leisure"](-33.04320549616556,114.69451904296876,-30.767799150881462,117.78717041015625);
      way["leisure"](-33.04320549616556,114.69451904296876,-30.767799150881462,117.78717041015625);
      node["public_transport"](-33.04320549616556,114.69451904296876,-30.767799150881462,117.78717041015625);
      way["public_transport"](-33.04320549616556,114.69451904296876,-30.767799150881462,117.78717041015625);
      node["highway"="bus_stop"](-33.04320549616556,114.69451904296876,-30.767799150881462,117.78717041015625);
      node["railway"="station"](-33.04320549616556,114.69451904296876,-30.767799150881462,117.78717041015625);
      way["railway"="station"](-33.04320549616556,114.69451904296876,-30.767799150881462,117.78717041015625);
      node["healthcare"](-33.04320549616556,114.69451904296876,-30.767799150881462,117.78717041015625);
      way["healthcare"](-33.04320549616556,114.69451904296876,-30.767799150881462,117.78717041015625);
    );
    out body;
    >;
    out skel qt;
    """
    
    osm_data_file = 'osm_nodes.json'
    if USE_LOCAL_FILES:
        with open(osm_data_file, 'r') as file:
            osm_data = json.load(file)
    else:
        osm_data = fetch_overpass_data(main_query, osm_data_file)
        with open('osm_nodes.json', 'w') as file:
            json.dump(osm_data, file, indent=2)

    # Convert the fetched data to GeoJSON format and save
    geojson_data = convert_to_geojson(osm_data)
    with open('osm_nodes.geojson', 'w') as file:
        json.dump(geojson_data, file, indent=2)

    # Process the fetched data
    coast_file_path = 'qgis_coast.geojson'  # Updated file path
    process_geojson_data('osm_nodes.geojson', coast_file_path)
