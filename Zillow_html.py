# Parse the repository URL to extract owner and repo name
import evadb
import pandas as pd
import pudb
import numpy as np
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
        print("CHECK1")
        cursor.query("DROP TABLE IF EXISTS home_data16;").df()

        cursor.query(f"""
        CREATE TABLE home_data16(
            name TEXT(1000)
            );
            """).df()
        
        print("CHECK2")
        
        np.seterr(divide='ignore', invalid='ignore')
        cursor.query("DROP TABLE IF EXISTS tableNew;").df()
        cursor.query(f"""
        CREATE TABLE tableNew (
            beds INTEGER,
            bath INTEGER,
            price INTEGER,
            date TEXT(1000));
            """).df()

        
        tableNew_query = cursor.query(f"""
                SELECT *
                FROM tableNew;
        """).df()
        
        print(tableNew_query)
        
        #Loading data from CSV file into the table
        cursor.query("LOAD CSV 'zillow_html_data.csv' INTO tableNew;").execute()
        
        cursor.query(f"""
            DROP FUNCTION IF EXISTS HomeSaleForecast;
            """).df()
        # pu.db
        #We use statsforecast engine to train a time serise forecast model for sale prices of home
        cursor.query("""
            CREATE FUNCTION HomeSaleForecast FROM
                (
                SELECT price,date
                FROM tableNew
            
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