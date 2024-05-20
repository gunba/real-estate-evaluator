import requests
import json
import time
from multiprocessing import Pool, Manager
import re

base_url = "https://reiwa.com.au/api/search/listing"
output_file = "reiwa_listings.json"

# Helper function to process prices from free-text input to int
def process_price(price):
    # Clean up the price string
    price = price.replace(',', '').lower()

    # Extract numbers
    numbers = re.findall(r'[\d.]+', price)
    if not numbers or len(numbers) == 0:
        return None

    try:
        # Convert extracted numbers to integers
        base_price = float(numbers[0])

        # Determine the base price
        if 1 <= base_price <= 2:
            base_price *= 1_000_000  # Treat 1 or 2 as millions
        elif 100 <= base_price < 1000:
            base_price *= 1_000  # Treat 100-999 as thousands
        elif base_price >= 1000:
            base_price = base_price  # Treat 1000 and above as is

        # Handle phrases like "low millions", "mid 600ks", "high 2 millions"
        match = re.search(r'\b(low|mid|high)\b', price)

        if match:
            modifier = match.group(1)
            if base_price >= 1000000:
                if modifier == 'low':
                    base_price += 250000
                elif modifier == 'mid':
                    base_price += 500000
                elif modifier == 'high':
                    base_price += 750000
            else:
                if modifier == 'low':
                    base_price += 25000
                elif modifier == 'mid':
                    base_price += 50000
                elif modifier == 'high':
                    base_price += 75000
        return base_price
    except Exception as e:
        print(e)
        return 0

def mine_listings(args):
    params, is_sold, min_price, max_price, listings = args
    params["SearchCriteria"]["MinPrice"] = min_price
    params["SearchCriteria"]["MaxPrice"] = max_price

    page = 1

    while True:
        params["PaginatedRequest"]["CurrentPage"] = page

        while True:
            try:
                response = requests.post(base_url, json=params)
                data = response.json()
                break
            except Exception as e:
                print(f"Request failed: {str(e)}. Retrying...")
                time.sleep(5)  # Wait before retrying the request

        if data["Success"]:
            results = data["Result"]["PaginatedResponse"]["Results"]
            total_results = data["Result"]["PaginatedResponse"]["TotalResults"]
            page_size = params["PaginatedRequest"]["PageSize"]
            total_pages = (total_results + page_size - 1) // page_size

            if total_results == 0:
                print(f"No listings found for price range {min_price} - {max_price}.")
                break

            print(f"Total results found for price range {min_price} - {max_price}: {total_results}")
            print(f"Scraping page {page} of {total_pages}")

            for result in results:
                listing = {
                    "reiwa_address": result["Address"],
                    "reiwa_price": (params["SearchCriteria"]["MinPrice"] + params["SearchCriteria"]["MaxPrice"]) // 2,
                    "reiwa_landsize": result.get("LandArea", 0),
                    "reiwa_latitude": result['Latitude'],
                    "reiwa_longitude": result['Longitude'],
                    "reiwa_bedrooms": result.get("Bedrooms", 0),
                    "reiwa_bathrooms": result.get("Bathrooms", 0),
                    "reiwa_parking": result.get("Carspaces", 0),
                    "reiwa_house_type": result["PropertyType"],
                    "reiwa_image_url": result["ListingImageUrls"][0] if result["ListingImageUrls"] else None,
                    "reiwa_details_url": result["PropertyDetailsURL"],
                    "reiwa_is_sold": is_sold,
                    "reiwa_floor_plan_count": result.get("FloorPlanCount", 0),
                    "reiwa_agency_name": result.get("AgencyName", ""),
                    "reiwa_agency_no": result.get("AgencyNo", ""),
                    "reiwa_pets_allowed": result.get("PetsAllowed", False),
                    "reiwa_suburb": result.get("Suburb", ""),
                    "reiwa_listing_price": process_price(result.get("DisplayPrice", "0")),
                    "reiwa_listing_id": result.get("ListingId", 0),
                }
                listings.append(listing)

            if page >= total_pages:
                break

            page += 1

def process_listings(is_sold):
    initial_params = {
        "UserInitiated": True,
        "PaginatedRequest": {
            "PageSize": 5000,
            "CurrentPage": 1,
            "SortBy": "default",
            "SortDirection": None
        },
        "SearchCriteria": {
            "SearchTerm": "Map area",
            "ListingCategory": "ForSale",
            "PropertyCategory": "Residential",
            "SearchType": "polygon",
            "MinPrice": 0,
            "MaxPrice": 10000,
            "IncludeSurroundingSuburbs": False,
            "MustHaveLandSize": True,
            "SearchCoordinates": [
                {
                    "Latitude": -31.519463657453,
                    "Longitude": 116.192012193436
                },
                {
                    "Latitude": -31.519463657453,
                    "Longitude": 115.519285753007
                },
                {
                    "Latitude": -32.8689590699165,
                    "Longitude": 115.519285753007
                },
                {
                    "Latitude": -32.8689590699165,
                    "Longitude": 116.192012193436
                }
            ],
            "View": "list",
            "IsSold": is_sold
        }
    }

    with Manager() as manager:
        listings = manager.list()
        price_ranges = [(price, price + 9999) for price in range(0, 10000000, 10000)]

        with Pool() as pool:
            pool.map(mine_listings, [(initial_params, is_sold, min_price, max_price, listings) for min_price, max_price in price_ranges])

        return list(listings)

if __name__ == "__main__":
    try:
        # Collect sold listings
        sold_listings = process_listings(True)
        print("Sold listings data mining completed.")

        # Collect for sale listings
        for_sale_listings = process_listings(False)
        print("For sale listings data mining completed.")

        # Combine sold and for sale listings
        all_listings = sold_listings + for_sale_listings

        with open(output_file, "w") as file:
            json.dump(all_listings, file, indent=4)

    except Exception as e:
        print(f"An error occurred: {str(e)}")