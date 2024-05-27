import json
import math
import re
from multiprocessing.pool import ThreadPool as Pool

import requests
from bs4 import BeautifulSoup
from geopy.distance import geodesic
from shapely.geometry import shape
from tqdm import tqdm

# Constants
CBD_COORDINATES = (-31.953512, 115.857048)
MAX_DISTANCE_KM = 100
EARTH_RADIUS_KM = 6371

# Map for converting text headers to meaningful variable names
HEADER_MAP = {
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
    "Average number of motor vehicles per dwelling": "abs_avg_vehicles_per_house",
}


# Utility Functions
def clean_key(key):
    """Cleans and normalizes a key for consistent usage."""
    key = re.sub(r"\[.*?\]|\(.*?\)", "", key)
    key = re.sub(r"[^\w\s]", "", key)
    return key.replace(" ", "_").lower().strip("_").replace("__", "_")


def clean_value(value):
    """Cleans and converts a value to an appropriate type."""
    value = value.replace(",", "").replace("-", "")
    if value.endswith("%"):
        return float(value[:-1]) / 100
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return 0


def extract_summary_data(summary_container):
    """Extracts summary data from the summary container."""
    summary_data = {}
    for table in summary_container.find_all("table", class_="summaryTable"):
        for row in table.find_all("tr"):
            th, td = row.find("th"), row.find("td")
            if th and td:
                key = th.get_text(strip=True)
                value = (
                    td.get_text(strip=True).replace("$", "").replace(",", "")
                )
                if key in HEADER_MAP and value != "null":
                    summary_data[HEADER_MAP[key]] = clean_value(value)
    return summary_data


def extract_table_view_data(tables_view):
    """Extracts detailed data from the tables view."""
    table_data = {}
    for table in tables_view.find_all("table"):
        header_key = "abs_sub_" + clean_key(
            table.find("th").get_text(strip=True)
        )
        for row in table.find_all("tr")[1:]:
            th = row.find("th", class_="firstCol")
            td = row.find_all("td")
            if th and td and "rowMessage" not in th.get("class", []):
                row_key = clean_key(th.get_text(strip=True))
                value = td[0].get_text(strip=True)
                percent = td[1].get_text(strip=True)
                if row_key != "null":
                    table_data[f"{header_key}_{row_key}_val"] = clean_value(
                        value
                    )
                    table_data[f"{header_key}_{row_key}_pct"] = (
                        clean_value(percent) / 100
                    )
    return table_data


def process_geometry(geometry):
    """Processes geometry data to extract coordinates and calculate area."""
    coordinates = []
    if geometry["type"] == "Polygon":
        coordinates = geometry["coordinates"]
    elif geometry["type"] == "MultiPolygon":
        coordinates = geometry["coordinates"][0]

    if coordinates:
        suburb_geometry = shape({"type": "Polygon", "coordinates": coordinates})
        suburb_area_sq_degrees = suburb_geometry.area
        suburb_area_km2 = (
            suburb_area_sq_degrees * (math.pi / 180) ** 2 * EARTH_RADIUS_KM**2
        )
    else:
        suburb_area_km2 = 0

    return coordinates, suburb_area_km2


def process_suburb(suburb):
    """Processes a single suburb to extract relevant data."""
    try:
        scc_code = suburb["scc_code"][0]
        scc_name = suburb["scc_name"][0].replace(" (WA)", "")
        lon, lat = suburb["geo_point_2d"]["lon"], suburb["geo_point_2d"]["lat"]
        distance_to_cbd = geodesic((lat, lon), CBD_COORDINATES).kilometers

        if distance_to_cbd > MAX_DISTANCE_KM:
            return None

        url = f"https://www.abs.gov.au/census/find-census-data/quickstats/2021/SAL{scc_code}"
        response = requests.get(url)
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.content, "html.parser")
        summary_container = soup.find("div", id="summary-container")
        tables_view = soup.find("div", id="tablesView")

        if not summary_container or not tables_view:
            return None

        coordinates, area_km2 = process_geometry(
            suburb["geo_shape"]["geometry"]
        )

        summary_data = {
            "abs_scc_code": scc_code,
            "abs_scc_name": scc_name,
            "abs_coordinates": coordinates,
            "abs_area_km2": area_km2,
        }
        summary_data.update(extract_summary_data(summary_container))
        summary_data.update(extract_table_view_data(tables_view))
        return summary_data

    except Exception as e:
        print(
            f"Error processing suburb: {suburb.get('scc_name', ['Unknown'])[0]}"
        )
        print(f"Error message: {str(e)}")
        return None


def main():
    """Main function to execute the data extraction and processing."""
    url = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-australia-state-suburb/exports/json?lang=en&refine=ste_name%3A%22Western%20Australia%22&facet=facet(name%3D%22ste_name%22%2C%20disjunctive%3Dtrue)&timezone=Australia%2FPerth"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
    else:
        print(f"Failed to download data. Status code: {response.status_code}")
        return

    with Pool() as pool:
        results = list(tqdm(pool.imap(process_suburb, data), total=len(data)))
        extracted_data = {
            result["abs_scc_name"]: result for result in results if result
        }

    with open("census_data_processed.json", "w") as file:
        json.dump(extracted_data, file, indent=2)

    print("Data extraction completed.")


if __name__ == "__main__":
    main()
