import requests
import json

def retrieve_crime_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for non-2xx status codes
        crime_data = response.json()
        return crime_data
    except requests.exceptions.RequestException as e:
        print("Error occurred while retrieving crime data:", e)
        return None

def save_crime_data(crime_data, file_path):
    try:
        with open(file_path, 'w') as file:
            json.dump(crime_data, file, indent=2)
        print("Crime data saved successfully.")
    except IOError as e:
        print("Error occurred while saving crime data:", e)

if __name__ == '__main__':
    url = 'https://www.police.wa.gov.au/apiws/CrimeStatsApi/GetLocalityCrimeStats/'
    file_path = 'crime_data.json'

    crime_data = retrieve_crime_data(url)
    if crime_data is not None:
        save_crime_data(crime_data, file_path)