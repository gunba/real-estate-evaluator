import json
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from tqdm import tqdm
from geopy.distance import geodesic
from shapely.geometry import shape
import math

# Constants
CBD_COORDINATES = (-31.953512, 115.857048)
MAX_DISTANCE_KM = 100
EARTH_RADIUS_KM = 6371

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

def clean_key(key):
    key = re.sub(r'\[.*?\]', '', key)
    key = re.sub(r'\(.*?\)', '', key)
    key = re.sub(r'[^\w\s]', '', key)
    key = key.replace(' ', '_').lower()
    key = key.rstrip('_')
    key = key.replace('__', '_').lower()
    return key

def clean_value(value):
    value = value.replace(',', '').replace('-', '').replace('_-_', '')
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
                value = value.replace('$', '').replace(',', '')
                if value == "null":
                    continue
                if key in header_map:
                    summary_data[header_map[key]] = clean_value(value)

def extract_table_view_data(tables_view, summary_data):
    tables = tables_view.find_all('table')
    for table in tables:
        th = table.find_all('th')[0]
        main_text = ''.join([str(item) for item in th.contents if isinstance(item, str)]).strip()
        header_key = 'abs_sub_' + clean_key(main_text)
        for row in table.find_all('tr')[1:]:
            th = row.find('th', class_='firstCol')
            td = row.find_all('td')
            if th and td and 'rowMessage' not in th.get('class', []):
                row_key = clean_key(th.get_text(strip=True))
                value = td[0].get_text(strip=True)
                if row_key != "null":
                    percent = td[1].get_text(strip=True)
                    value = clean_value(value)
                    percent = clean_value(percent)
                    summary_data[header_key + '_' + row_key + '_val'] = value
                    summary_data[header_key + '_' + row_key + '_pct'] = percent / 100

def process_geometry(geometry):
    if geometry['type'] == 'Polygon':
        coordinates = geometry['coordinates']
    elif geometry['type'] == 'MultiPolygon':
        coordinates = geometry['coordinates'][0]
    else:
        coordinates = []

    if coordinates:
        suburb_geometry = shape({'type': 'Polygon', 'coordinates': coordinates})
        suburb_area_sq_degrees = suburb_geometry.area
        suburb_area_km2 = suburb_area_sq_degrees * (math.pi / 180) ** 2 * EARTH_RADIUS_KM ** 2
    else:
        suburb_area_km2 = 0

    return coordinates, suburb_area_km2

def process_suburb(suburb):
    try:
        scc_code = suburb['scc_code'][0]
        scc_name = suburb['scc_name'][0]
        lon = suburb['geo_point_2d']['lon']
        lat = suburb['geo_point_2d']['lat']
        distance_to_cbd = geodesic((lat, lon), CBD_COORDINATES).kilometers

        if distance_to_cbd > MAX_DISTANCE_KM:
            return None

        scc_name = scc_name.replace(" (WA)", "")
        url = f"https://www.abs.gov.au/census/find-census-data/quickstats/2021/SAL{scc_code}"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        summary_container = soup.find('div', id='summary-container')
        tables_view = soup.find('div', id='tablesView')

        if not summary_container or not tables_view:
            return None

        coordinates, area_km2 = process_geometry(suburb['geo_shape']['geometry'])

        summary_data = {
            'abs_scc_code': scc_code,
            'abs_scc_name': scc_name,
            'abs_coordinates': coordinates,
            'abs_area_km2': area_km2
        }
        extract_summary_data(summary_container, summary_data)
        extract_table_view_data(tables_view, summary_data)
        return summary_data

    except Exception as e:
        print(f"Error processing suburb: {scc_name}")
        print(f"Error message: {str(e)}")
        return None

url = 'https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-australia-state-suburb/exports/json?lang=en&refine=ste_name%3A%22Western%20Australia%22&facet=facet(name%3D%22ste_name%22%2C%20disjunctive%3Dtrue)&timezone=Australia%2FPerth'
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
else:
    print(f'Failed to download data. Status code: {response.status_code}')
    data = []

start_index = 0

with ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_suburb, suburb) for suburb in data[start_index:]]
    extracted_data = {}
    for future in tqdm(as_completed(futures), total=len(futures)):
        result = future.result()
        if result is not None:
            extracted_data[result["abs_scc_name"]] = result

with open('census_data_processed.json', 'w') as file:
    json.dump(extracted_data, file, indent=2)

print("Data extraction completed.")
