import json
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

# Map for converting text headers to meaningful variable names
header_map = {
    "People": "abs_people",
    "Male": "abs_male_ratio",
    "Female": "abs_female_ratio",
    "Median age": "abs_median_age",
    "Families": "abs_families",
    "for families with children": "abs_child_per_family",
    "for all households (a)": "abs_child_per_household",
    "All private dwellings": "abs_houses",
    "Average number of people per household": "abs_people_per_household",
    "Median weekly household income": "abs_median_weekly_household_income",
    "Median monthly mortgage repayments": "abs_median_monthly_mortgage_repayment",
    "Median weekly rent (b)": "abs_median_weekly_rent",
    "Average number of motor vehicles per dwelling": "abs_avg_vehicles_per_house"
}

def log_and_print(message):
    log_file.write(message + "\n")
    print(message)

def clean_key(key):
    # Remove brackets and content inside
    key = re.sub(r'\[.*?\]', '', key)
    key = re.sub(r'\(.*?\)', '', key)
    # Remove special characters and replace spaces with underscores
    key = re.sub(r'[^\w\s]', '', key)
    key = key.replace(' ', '_').lower()
    # Remove trailing underscores
    key = key.rstrip('_')
    # Remove double underscores
    key = key.replace('__', '_').lower()
    return key

def clean_value(value):
    # Remove commas, hyphens, and replace '_-_'
    value = value.replace(',', '').replace('-', '').replace('_-_', '')
    # Convert to appropriate type or replace with 0 if not numeric
    if value.endswith('%'):
        value = float(value[:-1]) / 100
    elif value.isdigit():
        value = int(value)
    elif value.replace('.', '', 1).isdigit():
        value = float(value)
    else:
        value = 0
    return value

def extract_summary_data(summary_container, summary_data):
    summary_tables = summary_container.find_all('table', class_='summaryTable')
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

                # Map headers to normal variable names
                if key in header_map:
                    summary_data[header_map[key]] = clean_value(value)

def extract_table_view_data(tables_view, summary_data):
    tables = tables_view.find_all('table')
    for table in tables:
        # Process each row of the table
        th = table.find_all('th')[0]

        # Extract the main text directly inside the <th> element
        main_text = ''.join([str(item) for item in th.contents if isinstance(item, str)]).strip()

        # Clean the key as before
        header_key = 'abs_sub_' + clean_key(main_text)

        summary_data[header_key] = {
            "pct": {}, 
            "val": {}
        }

        for row in table.find_all('tr')[1:]:  # Skip header row
            th = row.find('th', class_='firstCol')
            td = row.find_all('td')
            if th and td and 'rowMessage' not in th.get('class', []):  # Skip rows with 'firstCol rowMessage'
                row_key = clean_key(th.get_text(strip=True))
                value = td[0].get_text(strip=True)

                if row_key != "null":
                    percent = td[1].get_text(strip=True)

                    # Clean the value
                    value = clean_value(value)
                    percent = clean_value(percent)
                    
                    # Store the data in the flat dictionary
                    summary_data[header_key]['val'][row_key] = value
                    summary_data[header_key]['pct'][row_key] = percent/100

def process_suburb(suburb):
    try:
        scc_code = suburb['scc_code'][0]
        scc_name = suburb['scc_name'][0]

        # Strip " (WA)" from the suburb name
        scc_name = scc_name.replace(" (WA)", "")

        # Construct the URL for the ABS website
        url = f"https://www.abs.gov.au/census/find-census-data/quickstats/2021/SAL{scc_code}"

        # Send a GET request to the URL
        response = requests.get(url)

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the summary container div
        summary_container = soup.find('div', id='summary-container')
        tables_view = soup.find('div', id='tablesView')

        if not summary_container or not tables_view:
            log_and_print(f"Skipping suburb: {scc_name} (Insufficient data)")
            return None

        # Extract the data from the summary tables
        summary_data = {
            'abs_scc_code': scc_code,
            'abs_scc_name': scc_name
        }
        extract_summary_data(summary_container, summary_data)

        # Extract the data from the tables view
        extract_table_view_data(tables_view, summary_data)

        log_and_print(f"Processed suburb: {scc_name}\n{json.dumps(summary_data, indent=2)}")

        return summary_data

    except Exception as e:
        log_and_print(f"Error processing suburb: {scc_name}")
        log_and_print(f"Error message: {str(e)}")
        return None

# Load the JSON data from the file
with open('georef-australia-state-suburb.json', 'r') as file:
    data = json.load(file)

# Set the starting index for fault tolerance (start at 1600 for fast debugging)
start_index = 0

# Open the log file in write mode (overwrites previous log)
with open('extraction_log.txt', 'w') as log_file:
    # Create a thread pool with a maximum of 10 worker threads
    with ThreadPoolExecutor() as executor:
        # Submit the tasks to the thread pool
        futures = [executor.submit(process_suburb, suburb) for suburb in data[start_index:]]

        # Initialize an empty list to store the extracted data
        extracted_data = {}

        # Process the results as they become available
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                extracted_data[result["abs_scc_name"]] = result

    # Save the extracted data to a new JSON file
    with open('extracted_data.json', 'w') as file:
        json.dump(extracted_data, file, indent=2)

    log_and_print("Data extraction completed.")
