# Parse the repository URL to extract owner and repo name
import evadb
import pandas as pd

repo_url=" https://www.zillow.com/homes/for_sale/"
parts = repo_url.strip("/").split("/")
repo_name = parts[-1]
print(repo_name);
DEFAULT_CSV_PATH = f"{repo_name}.csv"

if __name__ == "__main__":
    try:
        # establish evadb api cursor
        print("⏳ Connect to EvaDB...")
        cursor = evadb.connect().cursor()
        print("✅ Connected to EvaDB...")

        cursor.query(f"DROP TABLE IF EXISTS home_data;").df()

        cursor.query(f"""
        CREATE TABLE home_data(
            name TEXT(1000)
            );
            """).df()
    #Loading the images
        print(cursor.query(f"""
            INSERT INTO home_data (name) VALUES ("Image11.png");
            """).df())
        print(cursor.query(f"""
            INSERT INTO home_data (name) VALUES ("Image22.png");
            """).df())
        print(cursor.query(f"""
            INSERT INTO home_data (name) VALUES ("Image33.png");
            """).df())
        print(cursor.query(f"""
            INSERT INTO home_data (name) VALUES ("Image44.png");
            """).df())
        print(cursor.query(f"""
            INSERT INTO home_data (name) VALUES ("Image55.png");
            """).df())
        print(cursor.query(f"""
            INSERT INTO home_data (name) VALUES ("Image66.png");
            """).df())
        print(cursor.query(f"""
            INSERT INTO home_data (name) VALUES ("Image77.png");
            """).df())
        print(cursor.query(f"""
            INSERT INTO home_data (name) VALUES ("Image88.png");
            """).df())
        print(cursor.query(f"""
            INSERT INTO home_data (name) VALUES ("Image99.png");
            """).df())
        print(cursor.query(f"""
            INSERT INTO home_data (name) VALUES ("Image1010.png");
            """).df())



        cursor.query(f"""
            DROP FUNCTION IF EXISTS WebPageTextExtractor;
            """).df()

        cursor.query(f"""
            DROP FUNCTION IF EXISTS StringToDataframe;
            """).df()


        cursor.query(f"""
            CREATE FUNCTION WebPageTextExtractor
            INPUT (urls TEXT(1000))
            OUTPUT (extracted_text TEXT(1000))
            TYPE  Webscraping
            IMPL  'testevadb/ImageToText_1.py';
        """).df()



        cursor.query(f"""
            CREATE FUNCTION StringToDataframe
            IMPL  'string_to_dataframe.py';
        """).df()

        cursor.query("DROP TABLE IF EXISTS data_extracted_23;").df()

    #Scraping the data from iages and storing it in table
        print(cursor.query(f"""
           CREATE TABLE IF NOT EXISTS data_extracted_23 AS
           SELECT name,WebPageTextExtractor(name)
           FROM home_data;
        """).df())

        pd.set_option('display.max_colwidth', None)
        select_query = cursor.query(f"SELECT * FROM data_extracted_23;").df()

        print(select_query)

        cursor.query("DROP TABLE IF EXISTS final_data_11;").df()

    #Using chatGPT to extract useful information from the scraped data
        cursor.query(f"""
            CREATE TABLE IF NOT EXISTS final_data_11 AS 
            SELECT StringToDataframe(
                ChatGPT("Extract these fields from the row: price, beds, bath, area, address, date. 
                  Do not give code or instructions. Return the fields.
                  Remove the S from price. Remove all the commas.
                  Remove sqft from area column values.
                  Here is an example (use it only for the output format, not for the content):

                price: $450,000
                beds: 3 bds
                bath: 1 ba
                area: 1600 sqft
                address: 5233 N Emerson Dr, Portland, OR 97217
                date: September 11, 2023
                
           
                
                
                ", data_extracted_23.extracted_text
                )
            )
            FROM data_extracted_23
            WHERE _row_id < 10;
        """).df()

        select_query = cursor.query(f"""
                SELECT *
                FROM final_data_11;
        """).df()

        print(select_query)

        np.seterr(divide='ignore', invalid='ignore')
        
        #We use statsforecast engine to train a time serise forecast model for sale prices of home
        cursor.query(f"""
        DROP FUNCTION IF EXISTS HomeSaleForecast;
        """).df()

        # pu.db
        cursor.query("""
            CREATE FUNCTION HomeSaleForecast FROM
                (
                SELECT price,date
                FROM final_data_11
            
                )
            TYPE Forecasting
            PREDICT 'price'
            HORIZON 4
            TIME 'date'
            FREQUENCY 'M';
        """).df()
        
        #We then use the HomeSaleForecast model to predict the sale price for homes for the next four months.
        
        output= cursor.query("""SELECT
                    HomeSaleForecast(4);""").df()
        print(output)
   

    except Exception as e:
        print(f"❗️ EvaDB Session ended with an error: {e}")
