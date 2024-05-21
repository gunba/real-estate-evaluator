import json

# Convert OSM tags into simple tags we can use for regression
def simplify_geojson(geojson_data):
    simplified_features = []
    for feature in geojson_data["features"]:
        properties = feature["properties"]
        geometry = feature["geometry"]
        simplified_properties = {"name": properties.get("name", "")}
        
        for key, value in properties.items():
            if key == "amenity":
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

# Convert full OSM data into streamlined data for regression
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

    with open('property_combinations.txt', 'w') as file:
        sorted_combinations = sorted(property_combinations.items(), key=lambda x: x[1], reverse=True)
        for combination, frequency in sorted_combinations:
            file.write(f"{combination}: {frequency}\n")

    geojson_data = simplify_geojson(geojson_data)

    with open('osm_nodes_processed.geojson', 'w') as file:
        json.dump(geojson_data, file, indent=2)

if __name__ == '__main__':
    file_path = 'osm_nodes.geojson'
    process_geojson_data(file_path)
