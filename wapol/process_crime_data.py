import json

def process_crime_data(crime_data):
    processed_data = {}

    for entry in crime_data:
        locality = entry['Locality']
        offense = entry['Offence']
        financial_year = entry['FinancialYear']
        total_annual = entry['TotalAnnual']

        if locality not in processed_data:
            processed_data[locality] = {}

        if financial_year not in processed_data[locality]:
            processed_data[locality][financial_year] = {
                "Locality": locality,
                "FinancialYear": financial_year,
                "Homicide": 0,
                "Sexual Offences": 0,
                "Assault (Family)": 0,
                "Assault (Non-Family)": 0,
                "Threatening Behaviour (Family)": 0,
                "Threatening Behaviour (Non-Family)": 0,
                "Deprivation of Liberty": 0,
                "Robbery": 0,
                "Dwelling Burglary": 0,
                "Non-Dwelling Burglary": 0,
                "Stealing of Motor Vehicle": 0,
                "Stealing": 0,
                "Property Damage": 0,
                "Arson": 0,
                "Drug Offences": 0,
                "Graffiti": 0,
                "Fraud & Related Offences": 0,
                "Breach of Violence Restraint Order": 0
            }

        processed_data[locality][financial_year][offense] += total_annual

    return processed_data

def save_processed_data(processed_data, file_path):
    try:
        with open(file_path, 'w') as file:
            json.dump(processed_data, file, indent=2)
        print("Processed crime data saved successfully.")
    except IOError as e:
        print("Error occurred while saving processed crime data:", e)

if __name__ == '__main__':
    with open('crime_data.json', 'r') as file:
        crime_data = json.load(file)

    processed_data = process_crime_data(crime_data)
    save_processed_data(processed_data, 'crime_data_processed.json')