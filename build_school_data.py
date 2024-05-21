import json
from geopy.distance import geodesic
from difflib import get_close_matches

# Load the student achievement data
with open('scsa/processed_student_achievement_data.json') as f:
    student_data = json.load(f)

# Load the geojson data
with open('osm/osm_nodes_processed.geojson', encoding='utf-8') as f:
    geo_data = json.load(f)

# Extract school features from geojson
schools = [feature for feature in geo_data['features'] if feature['properties'].get('primary_education') == 1]

# Function to find closest school name match
def find_closest_school(school_name, school_list):
    school_names = [school['properties']['name'] for school in school_list]
    # Ensure the school_name is a string
    if isinstance(school_name, str):
        closest_matches = get_close_matches(school_name, school_names, n=1, cutoff=0.6)
        return closest_matches[0] if closest_matches else None
    return None

# Combine data
combined_data = []

for student in student_data:
    school_name = student['scsa_school']
    closest_school_name = find_closest_school(school_name, schools)
    if closest_school_name:
        matching_school = next(school for school in schools if school['properties']['name'] == closest_school_name)
        longitude, latitude = matching_school['geometry']['coordinates']
        combined_entry = {
            'school_name': school_name,
            'longitude': longitude,
            'latitude': latitude,
            'achievement_data': student
        }
        combined_data.append(combined_entry)

# Save the combined data to a new JSON file
with open('school_data.json', 'w') as f:
    json.dump(combined_data, f, indent=4)

print("Combined data saved to school_data.json")
