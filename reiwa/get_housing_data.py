import json
import re
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

def extract_stat_value(soup, label_text):
    stat_box_label = soup.find("span", class_="o-stat-box__lbl", text=label_text)
    if stat_box_label:
        stat_box_value = stat_box_label.find_previous("strong", class_="o-stat-box__value").text.strip()
        return stat_box_value
    return None

def get_reiwa_suburb(suburb_name_raw):
    suburb_name = suburb_name_raw.lower().replace(" ", "-")
    print(suburb_name)
    
    url = f"https://reiwa.com.au/suburb/{suburb_name}/"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    data = {}

    # Local Government
    local_government_label = soup.find("p", class_="text-reset u-grey-dark stat__label", text=lambda text: "Local government" in text)
    if local_government_label:
        local_government_element = local_government_label.find_next_sibling("p", class_="text-reset u-flex-grow u-text-right-l")
        if local_government_element:
            data["reiwa_local_government"] = local_government_element.text.strip()
        else:
            data["reiwa_local_government"] = ""
    else:
        data["reiwa_local_government"] = ""

    # Median Sales Price
    median_sales_price_text = extract_stat_value(soup, "* Median sales price")
    if median_sales_price_text:
        median_sales_price_value = re.sub(r'[^\d.]', '', median_sales_price_text)
        if 'm' in median_sales_price_text.lower():
            data["reiwa_median_house_sale"] = int(float(median_sales_price_value) * 1000000)
        else:
            data["reiwa_median_house_sale"] = int(float(median_sales_price_value) * 1000)
    else:
        data["reiwa_median_house_sale"] = 0
        
    # Sales Growth
    sales_growth_text = extract_stat_value(soup, "Sales growth")
    if sales_growth_text:
        data["reiwa_sales_growth"] = float(sales_growth_text.replace("%", ""))
    else:
        data["reiwa_sales_growth"] = 0.0

    # Suburb Interest Level
    interest_level_element = soup.select_one("div[data-react-type='Insights/Suburb/InterestLevels']")
    if interest_level_element:
        interest_level_props = json.loads(interest_level_element["data-props"].replace("&quot;", "\""))
        data["reiwa_suburb_interest_level"] = interest_level_props["interestLevel"]
    else:
        data["reiwa_suburb_interest_level"] = ""

    return data

def fetch_suburb_data(suburb):
    try:
        return suburb['abs_scc_name'], get_reiwa_suburb(suburb['abs_scc_name'])
    except Exception as e:
        print(f"Error occurred for suburb {suburb['abs_scc_name']}:", e)
        return suburb['abs_scc_name'], None

# Load the census data from abs/extracted_data.json
with open('../abs/census_data_processed.json', 'r') as file:
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
