import json
import datetime
import os

# Load the crime data from wapol/crime_data_processed.json
with open('wapol/crime_data_processed.json', 'r') as file:
    crime_data = json.load(file)

# Load the census data from abs/extracted_data.json
with open('abs/extracted_data.json', 'r') as file:
    census_data = json.load(file)

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
    'Homicide': 'offences_homicide',
    'Sexual Offences': 'offences_sexual',
    'Assault (Family)': 'offences_assault_family',
    'Assault (Non-Family)': 'offences_assault_non_family',
    'Threatening Behaviour (Family)': 'offences_threatening_behaviour_family',
    'Threatening Behaviour (Non-Family)': 'offences_threatening_behaviour_non_family',
    'Deprivation of Liberty': 'offences_deprivation_of_liberty',
    'Robbery': 'offences_robbery',
    'Dwelling Burglary': 'offences_dwelling_burglary',
    'Non-Dwelling Burglary': 'offences_non_dwelling_burglary',
    'Stealing of Motor Vehicle': 'offences_stealing_motor_vehicle',
    'Stealing': 'offences_stealing',
    'Property Damage': 'offences_property_damage',
    'Arson': 'offences_arson',
    'Drug Offences': 'offences_drug',
    'Graffiti': 'offences_graffiti',
    'Fraud & Related Offences': 'offences_fraud',
    'Breach of Violence Restraint Order': 'offences_breach_vro'
}

# Define the crime categories
crime_categories = {
    'ap': ['offences_non_dwelling_burglary', 'offences_property_damage', 'offences_stealing_motor_vehicle', 'offences_arson', 'offences_dwelling_burglary'],
    'apn': ['offences_homicide', 'offences_assault_non_family', 'offences_deprivation_of_liberty', 'offences_sexual', 'offences_threatening_behaviour_family', 'offences_robbery', 'offences_assault_family', 'offences_threatening_behaviour_non_family']
}

# Initialize the aggregated suburb data
aggregated_suburb_data = {}

# Iterate over the suburbs in the crime data
for suburb, data in crime_data.items():
    # Check if the suburb has data for the current and previous financial years
    if current_fy in data and previous_fy in data:
        current_data = data[current_fy]
        previous_data = data[previous_fy]

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

        # Calculate the increase in crime rate for each category
        crime_rate_inc = {
            cat: (
                (current_cat_crime[cat] - previous_cat_crime[cat]) / previous_cat_crime[cat]
                if previous_cat_crime[cat] != 0
                else 0
            )
            for cat in crime_categories
        }

        # Find the matching suburb in the census data (case-insensitive)
        matching_suburb = next(
            (suburb_data for suburb_name, suburb_data in census_data.items() if suburb_name.lower() == suburb.lower()),
            None
        )

        if matching_suburb:
            # Combine the crime data and census data for the suburb
            aggregated_suburb_data[matching_suburb['scc_name']] = {
                **updated_current_data,
                'ap_crime': current_cat_crime['ap'],
                'apn_crime': current_cat_crime['apn'],
                'ap_crime_inc': crime_rate_inc['ap'],
                'apn_crime_inc': crime_rate_inc['apn'],
                **{k: v for k, v in matching_suburb.items() if k not in ['scc_code', 'scc_name']}
            }

# Save the aggregated suburb data to a new JSON file
with open('suburb_data.json', 'w') as file:
    json.dump(aggregated_suburb_data, file, indent=2)

print("Aggregated suburb data saved to 'suburb_data.json'.")