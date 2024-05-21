import json
import re
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_reiwa_suburb(suburb_name_raw):
    suburb_name = suburb_name_raw.lower().replace(" ", "-")
    print(suburb_name)
    
    url = f"https://reiwa.com.au/suburb/{suburb_name}/"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    data = {}

    # Local Government
    data["reiwa_local_government"] = soup.select("p.text-reset.u-flex-grow.u-text-right-l")[2].text.strip()

    # Sales Growth
    sales_growth_element = soup.select(".o-stat-box.-aqua strong.o-stat-box__value")
    if sales_growth_element:
        sales_growth_text = sales_growth_element[0].text.strip()
        data["reiwa_sales_growth"] = float(sales_growth_text.replace("%", ""))
    else:
        data["reiwa_sales_growth"] = 0.0

    median_sales_price_element = soup.select(".o-stat-box.-aqua strong.o-stat-box__value")
    if len(median_sales_price_element) > 1:
        median_sales_price_text = median_sales_price_element[1].text.strip()
        median_sales_price_value = re.sub(r'[^\d.]', '', median_sales_price_text)
        if 'm' in median_sales_price_text.lower():
            data["reiwa_median_house_sale"] = int(float(median_sales_price_value) * 1000000)
        else:
            data["reiwa_median_house_sale"] = int(float(median_sales_price_value) * 1000)
    else:
        data["reiwa_median_house_sale"] = 0
    
    # Suburb Interest Level
    interest_level_element = soup.select_one("div[data-react-type='Insights/Suburb/InterestLevels']")
    interest_level_props = json.loads(interest_level_element["data-props"].replace("&quot;", "\""))
    data["reiwa_suburb_interest_level"] = interest_level_props["interestLevel"]

    return data

def fetch_suburb_data(suburb):
    try:
        return suburb['abs_scc_name'], get_reiwa_suburb(suburb['abs_scc_name'])
    except Exception as e:
        print(f"Error occurred for suburb {suburb['abs_scc_name']}:", e)
        return suburb['abs_scc_name'], None

# Load the census data from abs/extracted_data.json
with open('../abs/extracted_data.json', 'r') as file:
    census_data = json.load(file)

# Initialize the REIWA housing data
reiwa_housing_data = {}

# Use ThreadPoolExecutor to fetch data concurrently
with ThreadPoolExecutor(max_workers=24) as executor:
    futures = [executor.submit(fetch_suburb_data, suburb) for suburb in census_data.values()]
    for future in as_completed(futures):
        suburb_name, data = future.result()
        if data:
            reiwa_housing_data[suburb_name] = data

# Save the REIWA housing data to a JSON file
with open('reiwa_housing_data.json', 'w') as file:
    json.dump(reiwa_housing_data, file, indent=2)

print("REIWA housing data saved to 'reiwa_housing_data.json'.")
