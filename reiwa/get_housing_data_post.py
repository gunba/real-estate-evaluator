import requests
import json
import time

base_url = "https://reiwa.com.au/api/search/listing"
output_file = "reiwa_listings.json"
listings = []

def get_price_increment(price):
    if price < 1000000:
        return 50000
    else:
        return (price // 1000000) * 100000

def mine_listings(params, is_sold):
    price = 100000
    while price <= 10000000:
        params["SearchCriteria"]["MinPrice"] = price
        params["SearchCriteria"]["MaxPrice"] = price + get_price_increment(price)

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
                    print("No listings found for the current search criteria.")
                    break

                print(f"Total results found: {total_results}")
                print(f"Scraping page {page} of {total_pages}")

                for result in results:
                    listing = {
                        "address": result["Address"],
                        "price": (params["SearchCriteria"]["MinPrice"] + params["SearchCriteria"]["MaxPrice"]) // 2,
                        "landsize": result.get("LandArea", 0),
                        "latitude": result['Latitude'],
                        "longitude": result['Longitude'],
                        "bedrooms": result.get("Bedrooms", 0),
                        "bathrooms": result.get("Bathrooms", 0),
                        "parking": result.get("Carspaces", 0),
                        "house_type": result["PropertyType"],
                        "image_url": result["ListingImageUrls"][0] if result["ListingImageUrls"] else None,
                        "details_url": result["PropertyDetailsURL"],
                        "is_sold": is_sold
                    }
                    listings.append(listing)

                with open(output_file, "w") as file:
                    json.dump(listings, file, indent=4)

                if page >= total_pages:
                    break

                page += 1

        price += get_price_increment(price)

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
        "MinPrice": 100000,
        "MaxPrice": 150000,
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
        "View": "list"
    }
}

try:
    # Collect sold listings
    initial_params["SearchCriteria"]["IsSold"] = True
    mine_listings(initial_params, is_sold=True)
    print("Sold listings data mining completed.")

    # Collect for sale listings
    initial_params["SearchCriteria"]["IsSold"] = False
    mine_listings(initial_params, is_sold=False)
    print("For sale listings data mining completed.")

except Exception as e:
    print(f"An error occurred: {str(e)}")