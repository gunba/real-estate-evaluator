import requests
import json
import time
from multiprocessing import Pool, Manager

base_url = "https://reiwa.com.au/api/search/listing"
output_file = "reiwa_listings.json"

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
                    "is_sold": is_sold,
                    "floor_plan_count": result.get("FloorPlanCount", 0),
                    "agency_name": result.get("AgencyName", ""),
                    "agency_no": result.get("AgencyNo", ""),
                    "pets_allowed": result.get("PetsAllowed", False),
                    "suburb": result.get("Suburb", "")
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
        price_ranges = [(price, price + 10000) for price in range(0, 10000000, 10000)]

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