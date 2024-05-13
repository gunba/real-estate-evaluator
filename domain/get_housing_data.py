from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import json
import time
import traceback

base_url = "https://www.domain.com.au/sale/"
initial_params = {
    "excludeunderoffer": "0",
    "startloc": "-31.10337177302691,113.91887657939456",
    "endloc": "-33.11666676829659,117.73387902080081",
    "ssubs": "0",
    "ptype": "apartment-unit-flat,block-of-units,duplex,free-standing,new-apartments,new-home-designs,new-house-land,pent-house,semi-detached,studio,terrace,town-house,villa",
}

output_file = "real_estate_data.json"
listing_data = []

chrome_options = Options()
chrome_options.add_argument("--blink-settings=imagesEnabled=false")
driver = webdriver.Chrome(options=chrome_options)
driver.set_page_load_timeout(30)

def scrape_listings(params):
    page = 1
    while True:
        url = base_url + "?" + "&".join(f"{key}={value}" for key, value in params.items()) + f"&page={page}"
        try:
            driver.get(url)
        except TimeoutException:
            print("Page load timed out. Proceeding with parsing.")

        try:
            total_properties_element = driver.find_element(By.CSS_SELECTOR, "h1[data-testid='summary'] strong")
            try:
                total_properties = int(total_properties_element.text.split()[0].replace(",", ""))
            except ValueError:
                print("Encountered 'undefined' properties. Refreshing the page.")
                driver.refresh()
                time.sleep(5)  # Wait for the page to refresh
                continue  # Restart the loop to retrieve the total properties again
            
            print(f"Total properties found: {total_properties}")
            
            if total_properties == 0:
                print("No properties found for the current search area.")
                break
            
            if total_properties > 1000:
                start_lat, start_lon = map(float, params["startloc"].split(","))
                end_lat, end_lon = map(float, params["endloc"].split(","))
                
                mid_lat = (start_lat + end_lat) / 2
                mid_lon = (start_lon + end_lon) / 2
                
                scrape_listings({**params, "endloc": f"{mid_lat},{mid_lon}"})
                scrape_listings({**params, "startloc": f"{mid_lat},{start_lon}", "endloc": f"{end_lat},{mid_lon}"})
                scrape_listings({**params, "startloc": f"{start_lat},{mid_lon}", "endloc": f"{mid_lat},{end_lon}"})
                scrape_listings({**params, "startloc": f"{mid_lat},{mid_lon}"})
                break
            
            if any("Try tweaking your search results criteria for more results" in p.text for p in driver.find_elements(By.TAG_NAME, "p")):
                print("Reached the end of search results. Moving to the next search.")
                break
            
            listing_items = driver.find_elements(By.CSS_SELECTOR, "li[data-testid^='listing-']")

            for item in listing_items:
                listing = {}

                try:
                    listing["id"] = item.get_attribute("data-testid").split("-")[-1]
                    listing_card = item.find_element(By.CSS_SELECTOR, "div")
                    listing["url"] = listing_card.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
                    listing["price"] = listing_card.find_element(By.CSS_SELECTOR, "p[data-testid='listing-card-price']").text.strip()

                    address_wrapper = listing_card.find_element(By.CSS_SELECTOR, "h2[data-testid='address-wrapper']")
                    address_spans = address_wrapper.find_elements(By.CSS_SELECTOR, "span[data-testid^='address-line']")
                    listing["address"] = " ".join(span.text.strip() for span in address_spans)
                    print(f"Loading data for: {listing['address']}")

                    features_wrapper = listing_card.find_element(By.CSS_SELECTOR, "div[data-testid='listing-card-features-wrapper']")
                    listing["house_type"] = features_wrapper.find_elements(By.TAG_NAME, "div")[-1].find_element(By.TAG_NAME, "span").text.strip()

                    features_wrapper = listing_card.find_element(By.CSS_SELECTOR, "div[data-testid='property-features-wrapper']")
                    feature_spans = features_wrapper.find_elements(By.CSS_SELECTOR, "span[data-testid='property-features-text-container']")
                    for span in feature_spans:
                        feature_value = span.text.strip().split(" ")[0]
                        try:
                            feature_text = span.find_element(By.CSS_SELECTOR, "span[data-testid='property-features-text']").text.strip()
                            listing[feature_text.lower()] = feature_value
                        except NoSuchElementException:
                            if "m²" in span.text.strip():
                                listing["area"] = feature_value

                    listing_data.append(listing)

                except Exception as e:
                    print(f"An error occurred while processing listing: {listing}")
                    print(f"Error message: {str(e)}")
                    print(f"Error traceback:")
                    traceback.print_exc()
                    continue

            print(f"Scraped page {page}")
            page += 1
            time.sleep(5)  # Delay between requests to avoid overwhelming the server

            with open(output_file, "w") as file:
                json.dump(listing_data, file, indent=4)

        except NoSuchElementException:
            print("No properties found for the current search area.")
            break

try:
    scrape_listings(initial_params)
    print("Scraping completed.")

except Exception as e:
    print(f"An error occurred: {str(e)}")
    print(f"Error traceback:")
    traceback.print_exc()

finally:
    print('Scraping completed. Press any key to exit.')
    input()
    driver.quit()