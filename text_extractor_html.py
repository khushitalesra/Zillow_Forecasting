from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
import concurrent.futures
import pandas as pd
import time
from evadb.catalog.catalog_type import ColumnType
from evadb.functions.abstract.abstract_function import AbstractFunction
from evadb.functions.decorators.decorators import forward, setup
from evadb.functions.decorators.io_descriptors.data_types import PandasDataframe

import easyocr

options = Options()


options.add_argument("--headless")
chromedriver_path = "/opt/homebrew/bin/chromedriver"
service = Service(chromedriver_path)


from tqdm import tqdm


# reader = easyocr.Reader(["en"], gpu=True)


def scrape_user_page(url):
    try:

        driver = webdriver.Chrome(service=service, options=options)


        driver.set_window_size(1920, 1080)
    
        print("check1")
        
        driver.get('file://'+"/Users/khushitalesra/Downloads/evadb/Zillow_Homesale_Forecast/websiteData.html")
        print("URL successfully loaded:", driver.current_url)
        
        user_info_blocks = []
        # try:
        
        price_elements = driver.find_elements(By.XPATH, "//span[@data-test='property-card-price']")
        for price_element in price_elements:
            price = price_element.text
            print("Price:", price)
        bedroom_elements = driver.find_elements(By.XPATH, "//li[abbr[contains(text(), 'bds')]]")
        for bedroom_element in bedroom_elements:
            bedroom = bedroom_element.text
            print("Bedroom:", bedroom)
        bathrooms_element = driver.find_elements(By.XPATH, "//li[abbr[contains(text(), 'ba')]]")
        for bathroom_element in bathrooms_element:
            bathroom = bathroom_element.text
            print("Bathroom:", bathroom)


        user_profile_elements = []

        try:
            # Find all the elements containing bedrooms, bathrooms, and prices
            bedrooms_elements = driver.find_elements(By.XPATH, "//li[abbr[contains(text(), 'bds')]]")
            bathrooms_elements = driver.find_elements(By.XPATH, "//li[abbr[contains(text(), 'ba')]]")
            prices_elements = driver.find_elements(By.XPATH, "//span[@data-test='property-card-price']")

            # Extract data and store it in the dictionary
            for i in range(len(bedrooms_elements)):
                bedrooms = bedrooms_elements[i].text.split()[0]
                bathrooms = bathrooms_elements[i].text.split()[0]
                price = prices_elements[i].text
                user_profile_elements.append({
                    'beds': bedrooms,
                    'bath': bathrooms,
                    'price': price
                })

        except Exception as e:
            print("Error:", e)
        print(user_profile_elements)
        
        for profile in user_profile_elements:
            profile['price'] = int(''.join(filter(str.isdigit, profile['price'])))
        df = pd.DataFrame(user_profile_elements)

        # Specify the CSV file path
        csv_file = '/Users/khushitalesra/Downloads/evadb/Zillow_Homesale_Forecast/zillow_html_data.csv'

        # Write DataFrame to CSV file
        df.to_csv(csv_file, index=False)

        print(f'Data has been saved to {csv_file}')


    except Exception as e:
        print(f"Error for {url}: {str(e)}")


if __name__ == "__main__":
    scrape_user_page("https://www.zillow.com/homes/for_sale/")