from selenium import webdriver
import json
import time

def convert_value(value):
    """Convert value to int or float, or return 0 if empty"""
    if value == "":
        return 0
    try:
        if '.' in value:
            return float(value)
        return int(value)
    except ValueError:
        return value

def process_record(record):
    """Process each record to prefix keys and convert values"""
    processed_record = {}
    for key, value in record.items():
        if key == 'medatar':
            continue  # Skip the 'medatar' key
        new_key = 'scsa_' + key
        processed_record[new_key] = convert_value(value)
    return processed_record

# Initialize the WebDriver (replace 'chromedriver' with the path to your WebDriver if necessary)
driver = webdriver.Chrome()  # For Firefox, use webdriver.Firefox()

# Navigate to the webpage
driver.get('https://senior-secondary.scsa.wa.edu.au/certification/student-achievement-data-by-school')

# Allow the page to load completely
time.sleep(5)

# Execute JavaScript to retrieve the data
json_data = driver.execute_script('return JSON.stringify(window.studentDataList);')

# Close the browser
driver.quit()

# Parse the JSON data
data = json.loads(json_data)

# Process the data
processed_data = [process_record(record) for record in data]

# Save the processed data to a JSON file
with open('processed_student_achievement_data.json', 'w') as json_file:
    json.dump(processed_data, json_file, indent=4)

print("Processed data has been saved to 'processed_student_achievement_data.json'")
