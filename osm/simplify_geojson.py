import json

def simplify_geojson(geojson_data):
    simplified_features = []

    for feature in geojson_data["features"]:
        properties = feature["properties"]
        geometry = feature["geometry"]

        simplified_properties = {}

        for key, value in properties.items():
            if key == "amenity":
                if value in ["cafe", "fast_food", "restaurant", "pub", "bar", "biergarten", "food_court", "ice_cream"]:
                    simplified_properties["food"] = 1
                elif value in ["parking", "parking_entrance", "parking_space", "motorcycle_parking", "bicycle_parking"]:
                    simplified_properties["parking"] = 1
                elif value in ["bench", "shelter", "lounger", "shower", "toilets", "drinking_water"]:
                    simplified_properties["public_facilities"] = 1
                elif value in ["hospital", "clinic", "doctors", "dentist", "pharmacy", "veterinary"]:
                    simplified_properties["healthcare"] = 1
                elif value in ["school", "kindergarten", "college", "university", "library"]:
                    simplified_properties["education"] = 1
                elif value in ["police", "fire_station", "post_office", "townhall", "courthouse", "community_centre"]:
                    simplified_properties["public_service"] = 1
                elif value in ["bank", "atm", "bureau_de_change"]:
                    simplified_properties["financial"] = 1
                elif value in ["place_of_worship", "monastery"]:
                    simplified_properties["religion"] = 1
                elif value in ["fuel"]:
                    simplified_properties["fuel"] = 1
                elif value in ["nightclub"]:
                    simplified_properties["nightclub"] = 1
                elif value in ["cinema", "theatre"]:
                    simplified_properties["movies"] = 1
                elif value in ["waste_basket", "waste_disposal", "dog_excrement", "recycling", "trash"]:
                    simplified_properties["waste"] = 1
                else:
                    simplified_properties["other_amenity"] = 1
            elif key.startswith("shop"):
                simplified_properties["shop"] = 1
            elif key.startswith("tourism"):
                simplified_properties["tourism"] = 1
            elif key.startswith("sport"):
                simplified_properties["sport"] = 1
            elif key.startswith("leisure"):
                simplified_properties["leisure"] = 1
            elif key.startswith("artwork_type"):
                simplified_properties["artwork"] = 1
            elif key == "highway" and value == "bus_stop":
                simplified_properties["bus_stop"] = 1
            elif key == "railway" and value == "station":
                simplified_properties["railway_station"] = 1
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
