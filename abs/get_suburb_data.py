import json
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# Map for converting text headers to meaningful variable names
header_map = {
    "People": "people",
    "Male": "male_ratio",
    "Female": "female_ratio",
    "Median age": "median_age",
    "Families": "families",
    "for families with children": "child_per_family",
    "for all households (a)": "child_per_household",
    "All private dwellings": "houses",
    "Average number of people per household": "people_per_household",
    "Median weekly household income": "median_weekly_household_income",
    "Median monthly mortgage repayments": "median_monthly_mortgage_repayment",
    "Median weekly rent (b)": "median_weekly_rent",
    "Average number of motor vehicles per dwelling": "avg_vehicles_per_house"
}

def log_and_print(message):
    log_file.write(message + "\n")
    print(message)

def process_suburb(suburb):
    try:
        scc_code = suburb['scc_code'][0]
        scc_name = suburb['scc_name'][0]

        # Construct the URL for the ABS website
        url = f"https://www.abs.gov.au/census/find-census-data/quickstats/2021/SAL{scc_code}"

        # Send a GET request to the URL
        response = requests.get(url)

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the summary container div
        summary_container = soup.find('div', id='summary-container')

        # Check if summary tables exist
        summary_tables = summary_container.find_all('table', class_='summaryTable')
        if not summary_tables:
            log_and_print(f"Skipping suburb: {scc_name} (Insufficient data)")
            return None

        # Extract the data from the summary tables
        summary_data = {            
            'scc_code': scc_code,
            'scc_name': scc_name
        }

        for table in summary_tables:
            for row in table.find_all('tr'):
                th = row.find('th')
                td = row.find('td')
                if th and td:
                    key = th.get_text(strip=True)
                    value = td.get_text(strip=True)

                    # Remove dollar signs and commas from values
                    value = value.replace('$', '').replace(',', '')

                    # ABS likes to use th+td with no value for headers
                    if value == "null":
                        continue
                    # Convert percentages to floats
                    elif value.endswith('%'):
                        value = float(value[:-1]) / 100
                    # Check if only digits -> int
                    elif value.isdigit():
                        value = int(value)
                    # Otherwise, if period -> float
                    elif value.find('.')!=-1:
                        value = float(value)

                    # Map headers to normal variable names
                    summary_data[
                        header_map[key]
                    ] = value

        # Strip " (WA)" from the suburb name
        scc_name = scc_name.replace(" (WA)", "")

        log_and_print(f"Processed suburb: {scc_name}\n{json.dumps(summary_data, indent=2)}")

        return summary_data

    except Exception as e:
        log_and_print(f"Error processing suburb: {scc_name}")
        log_and_print(f"Error message: {str(e)}")
        return None

# Load the JSON data from the file
with open('georef-australia-state-suburb.json', 'r') as file:
    data = json.load(file)

# Set the starting index for fault tolerance
start_index = 0

# Open the log file in write mode (overwrites previous log)
with open('extraction_log.txt', 'w') as log_file:
    # Create a thread pool with a maximum of 10 worker threads
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit the tasks to the thread pool
        futures = [executor.submit(process_suburb, suburb) for suburb in data[start_index:]]

        # Initialize an empty list to store the extracted data
        extracted_data = {}

        # Process the results as they become available
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                extracted_data[result["scc_name"]] = result

    # Save the extracted data to a new JSON file
    with open('extracted_data.json', 'w') as file:
        json.dump(extracted_data, file, indent=2)

    log_and_print("Data extraction completed.")