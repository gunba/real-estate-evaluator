import json
import datetime
import os
import re
from multiprocessing.pool import ThreadPool

def process_suburb(suburb):
    crime_data = suburb[1]

    # Check if the suburb has data for the current and previous financial years
    if current_fy in crime_data and previous_fy in crime_data:
        current_data = crime_data[current_fy]
        previous_data = crime_data[previous_fy]

        # Create dictionaries to store the updated crime type keys for current and previous data
        updated_current_data = {}
        updated_previous_data = {}

        # Scale up the crime numbers for the current year and update the keys
        for crime_type, count in current_data.items():
            if crime_type not in ['Locality', 'FinancialYear']:
                updated_key = crime_type_mapping[crime_type]
                updated_current_data[updated_key] = int(count * scaling_factor)

        # Update the keys for the previous year data
        for crime_type, count in previous_data.items():
            if crime_type not in ['Locality', 'FinancialYear']:
                updated_key = crime_type_mapping[crime_type]
                updated_previous_data[updated_key] = count

        # Calculate the categorical crime numbers for the current year
        current_cat_crime = {
            cat: sum(updated_current_data[crime_type] for crime_type in crime_types)
            for cat, crime_types in crime_categories.items()
        }

        # Calculate the categorical crime numbers for the previous year
        previous_cat_crime = {
            cat: sum(updated_previous_data[crime_type] for crime_type in crime_types)
            for cat, crime_types in crime_categories.items()
        }

        # Find the matching suburb in the census data (case-insensitive)
        matching_suburb = next(
            (suburb_data for suburb_name, suburb_data in census_data.items() if suburb_name.lower() == suburb[0].lower()),
            None
        )

        if matching_suburb:
            # Get the REIWA suburb data
            reiwa_suburb_data = reiwa_housing_data.get(matching_suburb['abs_scc_name'], {})

            # Combine the crime data, census data, and REIWA suburb data for the suburb
            return (matching_suburb['abs_scc_name'], {
                **updated_current_data,
                'wapol_total_person_crime': current_cat_crime['ap'],
                'wapol_total_property_crime': current_cat_crime['apn'],
                'wapol_avg_person_crime_prev_3y': previous_cat_crime['ap'],
                'wapol_avg_property_crime_prev_3y': previous_cat_crime['apn'],
                **{k: v for k, v in matching_suburb.items() if k not in ['abs_scc_code', 'abs_scc_name']},
                **reiwa_suburb_data
            })

    return None

# Load the crime data from wapol/crime_data_processed.json
with open('wapol/crime_data_processed.json', 'r') as file:
    crime_data = json.load(file)

# Load the census data from abs/extracted_data.json
with open('abs/extracted_data.json', 'r') as file:
    census_data = json.load(file)

# Load the REIWA housing data from reiwa/reiwa_housing_data.json
with open('reiwa/reiwa_housing_data.json', 'r') as file:
    reiwa_housing_data = json.load(file)

# Get the current financial year
current_year = datetime.datetime.now().year
current_month = datetime.datetime.now().month
if current_month < 7:
    current_fy = f"{current_year - 1}-{str(current_year)[-2:]}"
    previous_fy = f"{current_year - 2}-{str(current_year - 1)[-2:]}"
else:
    current_fy = f"{current_year}-{str(current_year + 1)[-2:]}"
    previous_fy = f"{current_year - 1}-{str(current_year)[-2:]}"

# Get the number of days elapsed in the current financial year
current_date = datetime.datetime.now()
start_date = datetime.datetime(current_year - 1, 7, 1) if current_month < 7 else datetime.datetime(current_year, 7, 1)
days_elapsed = (current_date - start_date).days
scaling_factor = 365 / days_elapsed

# Define the crime type mapping
crime_type_mapping = {
    'Homicide': 'wapol_offences_homicide',
    'Sexual Offences': 'wapol_offences_sexual',
    'Assault (Family)': 'wapol_offences_assault_family',
    'Assault (Non-Family)': 'wapol_offences_assault_non_family',
    'Threatening Behaviour (Family)': 'wapol_offences_threatening_behaviour_family',
    'Threatening Behaviour (Non-Family)': 'wapol_offences_threatening_behaviour_non_family',
    'Deprivation of Liberty': 'wapol_offences_deprivation_of_liberty',
    'Robbery': 'wapol_offences_robbery',
    'Dwelling Burglary': 'wapol_offences_dwelling_burglary',
    'Non-Dwelling Burglary': 'wapol_offences_non_dwelling_burglary',
    'Stealing of Motor Vehicle': 'wapol_offences_stealing_motor_vehicle',
    'Stealing': 'wapol_offences_stealing',
    'Property Damage': 'wapol_offences_property_damage',
    'Arson': 'wapol_offences_arson',
    'Drug Offences': 'wapol_offences_drug',
    'Graffiti': 'wapol_offences_graffiti',
    'Fraud & Related Offences': 'wapol_offences_fraud',
    'Breach of Violence Restraint Order': 'wapol_offences_breach_vro'
}

# Define the crime categories
crime_categories = {
    'ap': ['wapol_offences_non_dwelling_burglary', 'wapol_offences_property_damage', 'wapol_offences_stealing_motor_vehicle', 'wapol_offences_arson', 'wapol_offences_dwelling_burglary'],
    'apn': ['wapol_offences_homicide', 'wapol_offences_assault_non_family', 'wapol_offences_deprivation_of_liberty', 'wapol_offences_sexual', 'wapol_offences_threatening_behaviour_family', 'wapol_offences_robbery', 'wapol_offences_assault_family', 'wapol_offences_threatening_behaviour_non_family']
}

# Initialize the aggregated suburb data
aggregated_suburb_data = {}

# Create a thread pool
pool = ThreadPool()

# Process the suburbs in parallel
results = pool.map(process_suburb, crime_data.items())

# Close the thread pool
pool.close()
pool.join()

# Filter out None results and add valid results to the aggregated suburb data
for result in results:
    if result is not None:
        suburb, data = result
        aggregated_suburb_data[suburb] = data

# Save the aggregated suburb data to a new JSON file
with open('suburb_data.json', 'w') as file:
    json.dump(aggregated_suburb_data, file, indent=2)

print("Aggregated suburb data saved to 'suburb_data.json'.")
