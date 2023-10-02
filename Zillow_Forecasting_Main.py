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

        # print(cursor.query(f"""
        #     SHOW TABLES;
        #     """).df())
        # pd.set_option('display.max_colwidth', None)
        # select_query = cursor.query(f"SELECT * FROM home_data;").df()

        # print(select_query)


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
        
        print(cursor.query(f"""
           CREATE TABLE IF NOT EXISTS data_extracted_23 AS
           SELECT name,WebPageTextExtractor(name)
           FROM home_data;
        """).df())

        pd.set_option('display.max_colwidth', None)
        select_query = cursor.query(f"SELECT * FROM data_extracted_23;").df()

        print(select_query)

        cursor.query("DROP TABLE IF EXISTS final_data_11;").df()

        cursor.query(f"""
            CREATE TABLE IF NOT EXISTS final_data_11 AS 
            SELECT StringToDataframe(
                ChatGPT("Extract these fields from the row: price, beds, bath, area, address. 
                Do not give code or instructions. Return the fields.
                Here is an example (use it only for the output format, not for the content):

                price: $450,000
                beds: 3 bds
                bath: 1 ba
                area: 1600 sqft
                address: 5233 N Emerson Dr, Portland, OR 97217
                
           
                
                
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

    #     select_query.to_csv("insights.csv", index=False)

    #     exit(0)

    #    # Create a dictionary to count starred repositories and the users who starred them
    #     repository_counts = {}

    #     # Iterate through users and their starred repositories
    #     for _, user in users_with_more_than_100_followers.iterrows():
    #         starred_repos = eval(user['stargazerdetails.user_starred_repos'])
            
    #         for repo in starred_repos:
    #             prefix = "https://github.com"
    #             repo_url = None

    #             for repo_element in list(repo):
    #                 if repo_element is None:
    #                     continue
    #                 if repo_element.startswith(prefix):
    #                     repo_url = repo_element

    #             if repo_url:
    #                 if repo_url not in repository_counts:
    #                     repository_counts[repo_url] = {'count': 1, 'users': {user['stargazerlist.github_username']}}
    #                 else:
    #                     repository_counts[repo_url]['count'] += 1
    #                     repository_counts[repo_url]['users'].add(user['stargazerlist.github_username'])

    #     # Sort the repositories by the number of users who have starred them
    #     sorted_repositories = sorted(repository_counts.items(), key=lambda x: x[1]['count'], reverse=True)

    #     # Specify the value of 'k' for the top k repositories
    #     k = 10

    #     # Get the top k repositories and their star counts
    #     top_k_repositories = sorted_repositories[:k]

    #     # Print the top k repositories and the number of users who have starred them
    #     for repo_url, data in top_k_repositories:
    #         star_count = data['count']
    #         star_users_count = len(data['users'])
            
    #         print(f"Repository URL: {repo_url}")
    #         print(f"Number of Users Who Starred It: {star_count}")
    #         print()

    except Exception as e:
        print(f"❗️ EvaDB Session ended with an error: {e}")