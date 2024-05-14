import json
import re
import requests
from collections import defaultdict

# Load the API key from the key.json file
with open('key.json', 'r') as key_file:
    key_data = json.load(key_file)
    API_KEY = key_data['API_KEY']

# Load the data from the JSON file
with open('real_estate_data.json', 'r') as file:
    data = json.load(file)

# Helper function to process prices
def process_price(price):
    # Clean up the price string
    price = price.replace(',', '').lower()

    # Extract numbers
    numbers = re.findall(r'[\d.]+', price)
    if not numbers or len(numbers) == 0:
        return None

    try:
        # Convert extracted numbers to integers
        base_price = float(numbers[0])

        # Determine the base price
        if 1 <= base_price <= 2:
            base_price *= 1_000_000  # Treat 1 or 2 as millions
        elif 100 <= base_price < 1000:
            base_price *= 1_000  # Treat 100-999 as thousands
        elif base_price >= 1000:
            base_price = base_price  # Treat 1000 and above as is

        # Handle phrases like "low millions", "mid 600ks", "high 2 millions"
        match = re.search(r'\b(low|mid|high)\b', price)

        if match:
            modifier = match.group(1)
            if base_price >= 1000000:
                if modifier == 'low':
                    base_price += 250000
                elif modifier == 'mid':
                    base_price += 500000
                elif modifier == 'high':
                    base_price += 750000
            else:
                if modifier == 'low':
                    base_price += 25000
                elif modifier == 'mid':
                    base_price += 50000
                elif modifier == 'high':
                    base_price += 75000
        return base_price
    except Exception as e:
        print(e)
        return 0


def geolocate_address(address, api_key):
    try:
        response = requests.get(f'https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={api_key}')
        results = response.json().get('results', [])
        if results:
            location = results[0]['geometry']['location']
            print(f"Geolocation result for address '{address}': {location['lat']}, {location['lng']}")
            return location['lat'], location['lng']
    except Exception as e:
        print(f"Error geolocating address {address}: {e}")
    return None, None

# Initialize a dictionary to track unique IDs and process the data
processed_data = {}
exclusions = defaultdict(list)
price_comparison = []

total_entries = len(data)
completed_entries = 0
for entry in data:
    entry_id = entry['id']
    if entry_id in processed_data:
        exclusions['duplicate_ids'].append(entry)
        completed_entries += 1
        print(f"Completed: {completed_entries}/{total_entries}, Remaining: {total_entries - completed_entries}")
        continue

    original_price = entry['price']
    price = process_price(original_price)
    if price is None or not (100_000 <= price <= 10_000_000):
        exclusions['invalid_price'].append(entry)
        completed_entries += 1
        print(f"Completed: {completed_entries}/{total_entries}, Remaining: {total_entries - completed_entries}")
        continue

    price_comparison.append(f'"{original_price}", {price}')

    beds = int(entry['beds'].split('\n')[0]) if 'beds' in entry and entry['beds'] else 0
    baths = int(entry['baths'].split('\n')[0]) if 'baths' in entry and entry['baths'] else 0
    parking = int(entry['parking'].split('\n')[0]) if 'parking' in entry and entry['parking'] else 0
    area = int(entry['area'].replace('m²', '').replace('\u00b2', '').replace(',', '')) if 'area' in entry and entry['area'] else 0

    address = entry['address']
    if ',' not in address:
        exclusions['invalid_address'].append(entry)
        completed_entries += 1
        print(f"Completed: {completed_entries}/{total_entries}, Remaining: {total_entries - completed_entries}")
        continue

    house_type = entry['house_type'].strip().title()
    if house_type in ["Townhouse", "Villa", "Retirement Living", "Apartment / Unit / Flat"]:
        house_type = "Unit"
    elif house_type == "New House And Land":
        house_type = "House"
    elif house_type not in ["House", "Unit"]:
        exclusions['invalid_house_type'].append(entry)
        completed_entries += 1
        print(f"Completed: {completed_entries}/{total_entries}, Remaining: {total_entries - completed_entries}")
        continue

    processed_data[entry_id] = {
        'id': entry_id,
        'url': entry['url'],
        'original_price': original_price,
        'price': price,
        'address': address,
        'latitude': None,
        'longitude': None,
        'house_type': house_type,
        'beds': beds,
        'baths': baths,
        'parking': parking,
        'area': area
    }

# Geolocate the addresses in a single thread
for entry in processed_data.values():
    address = entry['address']
    lat, lng = geolocate_address(address, API_KEY)
    entry['latitude'] = lat
    entry['longitude'] = lng

# Save the processed data to a new JSON file
processed_data_list = list(processed_data.values())
with open('real_estate_data_processed.json', 'w') as file:
    json.dump(processed_data_list, file, indent=4)

# Save the price comparisons to a text file
with open('price_comparison.txt', 'w') as file:
    for line in price_comparison:
        file.write(line + '\n')

# Save the exclusions to separate JSON files
for exclusion_type, entries in exclusions.items():
    with open(f'real_estate_data_{exclusion_type}.json', 'w') as file:
        json.dump(entries, file, indent=4)