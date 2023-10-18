from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from PIL import Image
import pytesseract
from io import BytesIO
import concurrent.futures
import pandas as pd
import time

from evadb.catalog.catalog_type import ColumnType
from evadb.functions.abstract.abstract_function import AbstractFunction
from evadb.functions.decorators.decorators import forward, setup
from evadb.functions.decorators.io_descriptors.data_types import PandasDataframe

# Initialize Selenium WebDriver
options = Options()
# Run in headless mode (no GUI)
options.add_argument("--headless")
# Replace with the path to your chromedriver executable
chromedriver_path = "/opt/homebrew/bin/chromedriver"
service = Service(chromedriver_path)

class WebPageTextExtractor(AbstractFunction):
    """
    Arguments:
        None

    Input Signatures:
        urls (list) : A list of URLs from which to extract text.

    Output Signatures:
        extracted_text (list) : A list of text extracted from the provided URLs.

    Example Usage:
        You can use this function to extract text from a list of URLs like this:

        urls = ["https://example.com/page1", "https://example.com/page2"]
    """

    @property
    def name(self) -> str:
        return "WebPageTextExtractor"

    @setup(cacheable=False, function_type="web-scraping")
    def setup(self) -> None:
        # Any setup or initialization can be done here if needed
        pass

    @forward(
        input_signatures=[
            PandasDataframe(
                columns=["urls"],
                column_types=[ColumnType.TEXT],
                column_shapes=[(None,)],
            )
        ],
        output_signatures=[
            PandasDataframe(
                columns=["extracted_text"],
                column_types=[ColumnType.TEXT],
                column_shapes=[(None,)],
            )
        ],
    )
    def forward(self, input_df):
        # Ensure URLs are provided
        if input_df.empty or input_df.iloc[0] is None:
            raise ValueError("URLs must be provided.")

        print(input_df)

        # Extract URLs from the DataFrame
        urls = input_df[0]
        print(urls)
        # Define a function to scrape user page and extract text
        def scrape_user_page(url):

            # full_url=url

            try:

                image = Image.open(url)

                # Use Pytesseract to extract text from the image
                extracted_text = pytesseract.image_to_string(image)

                return extracted_text

            except Exception as e:
                print(f"Error: {str(e)}")
            finally:
                driver.quit()


        # Define a function to extract text from a set of URLs
        def extract_text_from_urls(urls):
            extracted_text_list = []
            for i in range(len(urls)):
                url = urls[i]
                print(i)

                # Avoid hitting API limit
                if i % 10 == 0:
                    time.sleep(30)

                try:
                    # Scrape user page using Selenium and Pytesseract
                    extracted_text = scrape_user_page(url)
                    extracted_text_list.append(extracted_text)
                except Exception as e:
                    i = i - 1 
                    # sleep for 5 minutes
                    time.sleep(300)
                    print(f"Error extracting text from {url}: {str(e)}")
            return extracted_text_list

        # Use ThreadPoolExecutor for concurrent processing
        num_workers = 1
        num_urls = len(urls)

        print(f"Extracting text from {num_urls} URLs using {num_workers} workers")

        # Determine the number of URLs to process for each worker
        urls_per_worker = num_urls // num_workers
        remaining_urls = num_urls % num_workers

        # Create a list of URL chunks for each worker
        url_chunks = [urls[i:i + urls_per_worker] for i in range(0, num_urls, urls_per_worker)]

        # Distribute any remaining URLs to the workers
        for i in range(remaining_urls):
            additional_urls = [urls[num_urls - remaining_urls + i]]
            url_chunks[i] += additional_urls

        # Use ThreadPoolExecutor for concurrent processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Submit tasks to extract text from each URL chunk
            extracted_text_lists = list(executor.map(extract_text_from_urls, url_chunks))

        # Flatten the list of extracted text lists
        extracted_text_list = [text for sublist in extracted_text_lists for text in sublist]

        # Create a DataFrame from the extracted text
        extracted_text_df = pd.DataFrame({"extracted_text": extracted_text_list})

        return extracted_text_df