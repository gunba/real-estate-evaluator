from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import json
import time
import traceback

# TODO: Divide up space so that there is less than 1000 records per query
base_url = "https://www.domain.com.au/sale/"
params = {
    "excludeunderoffer": "0",
    "startloc": "-31.10337177302691,113.91887657939456",
    "endloc": "-33.11666676829659,117.73387902080081",
}

max_pages = 100  # Set the maximum number of pages to scan
output_file = "real_estate_data.json"
listing_data = []

# Set up Chrome options to disable image loading
chrome_options = Options()
chrome_options.add_argument("--blink-settings=imagesEnabled=false")

# Set up the Selenium webdriver with the configured options
driver = webdriver.Chrome(options=chrome_options)
driver.set_page_load_timeout(30)

try:
    for page in range(1, max_pages + 1):
        url = base_url + "?" + "&".join(f"{key}={value}" for key, value in params.items()) + f"&page={page}"
        try:
            driver.get(url)
        except TimeoutException:
            print("Page load timed out. Proceeding with parsing.")

        paragraph_element = driver.find_elements(By.TAG_NAME, "p")
        paragraph_found = False
        for p in paragraph_element:
            if "The requested URL was not found on the server." in p.text:
                paragraph_found = True
                break

        if not paragraph_found:
            listing_items = driver.find_elements(By.CSS_SELECTOR, "li[data-testid^='listing-']")

            for item in listing_items:
                listing = {}

                try:
                    listing_id = item.get_attribute("data-testid").split("-")[-1]
                    listing["id"] = listing_id

                    listing_card = item.find_element(By.CSS_SELECTOR, "div")
                    listing["url"] = listing_card.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
                    listing["price"] = listing_card.find_element(By.CSS_SELECTOR, "p[data-testid='listing-card-price']").text.strip()

                    address_wrapper = listing_card.find_element(By.CSS_SELECTOR, "h2[data-testid='address-wrapper']")
                    address_spans = address_wrapper.find_elements(By.CSS_SELECTOR, "span[data-testid^='address-line']")
                    address = " ".join(span.text.strip() for span in address_spans)
                    listing["address"] = address
                    print(f"Loading data for: {address}")

                    features_wrapper = listing_card.find_element(By.CSS_SELECTOR, "div[data-testid='listing-card-features-wrapper']")
                    house_type = features_wrapper.find_elements(By.TAG_NAME, "div")[2].find_element(By.TAG_NAME, "span").text.strip()
                    listing["house_type"] = house_type

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
            time.sleep(5)  # Delay between requests to avoid overwhelming the server

            # Save the listing data to a JSON file after each page
            with open(output_file, "w") as file:
                json.dump(listing_data, file, indent=4)
        else:
            print(f"Found end of listings on page {page}")
            break

    print("Scraping completed.")

except Exception as e:
    print(f"An error occurred: {str(e)}")
    print(f"Error traceback:")
    traceback.print_exc()

finally:
    print('Scraping completed. Press any key to exit.')
    input()
    driver.quit()