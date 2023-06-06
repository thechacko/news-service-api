from loguru import logger
import pandas as pd
import numpy as np
import sys
from tqdm import tqdm
import time
from datetime import datetime
import configparser
from utils.scraper import fetch_url_response, save_rawd, fetch_fix_rawd, scrape_process
from utils.queries import *
from utils.helper import check_conn, create_table, fill_data, fetch_data, process_feature, perform_ner, perform_sentiment
import warnings
warnings.filterwarnings('ignore')


# Set up configurations
configs = configparser.ConfigParser()
configs.read(["../configs/database.ini"])
db_configs = {}
db_configs["dbname"] = configs["PostgreSQL"]["dbname"]
db_configs["user"] = configs["PostgreSQL"]["user"]
db_configs["password"] = configs["PostgreSQL"]["password"]
db_configs["host"] = configs["PostgreSQL"]["host"]
db_configs["port"] = configs["PostgreSQL"]["port"] 


# Set Logger
today = datetime.today().strftime("%Y-%m-%d")
logger.add(f"../logs/{today}.log", rotation = "1 day", retention = "7 days", level = "INFO")
logger.info("Pipeline Initiates")


# Get response from the url
url = "https://www.livemint.com/sitemap/yesterday.xml"
response = fetch_url_response(url = url)
#print(response)

# Parse and save raw html from the response
save_rawd(response = response)

# Get the data from the raw html (create a cache function to store the data and delete after 7 days)
data_path = "..\data"
loaded_content = fetch_fix_rawd(directory = data_path)
#print(loaded_content)

# Process the data retrieved from the raw html
df = scrape_process(loaded_content = loaded_content)

# Check for Database connection 
check_conn(**db_configs)

# Check for table: staging.news, if not create it
create_table(schema = SCHEMA_1, table = TABLE_1, query_check = CHECK_TABLE_1, 
             query_create = CREATE_TABLE_1, **db_configs)

# Insert the processed data into the table
fill_data(df = df, schema = SCHEMA_1, table = TABLE_1, query = fill_query, **db_configs)

# Fetch the data and perform NER
rawd_2, cols = fetch_data(schema = SCHEMA_1, table = TABLE_1, query = fetch_query, query_type = "fetch", **db_configs)
df_sn = pd.DataFrame(rawd_2, columns = cols).drop("id", axis = 1)
#df_fetch.to_csv("../data/fetch_test_data.csv")
#print(df_sn.columns)

df_eng = process_feature(df = df_sn)
#print(df_eng.columns)
#df_eng.to_csv("../data/fetch_test_data.csv")

df_ner = perform_ner(df = df_eng)
#print(df_ner.columns) 
#print(df_ner["keywords"][0])

# Check for table: staging.ner, if not create it 
create_table(schema = SCHEMA_1, table= TABLE_2, 
             query_check = CHECK_TABLE_2, query_create = CREATE_TABLE_2, **db_configs) 

# Insert the processed data into the table
fill_data(df = df_ner, schema = SCHEMA_1, table = TABLE_2, query = fill_query, **db_configs)

# Fetch the data and perform sentiment analysis on the data
rawd_3, cols = fetch_data(schema = SCHEMA_1, table = TABLE_2, query = fetch_query, query_type = "fetch", **db_configs)
df_sne = pd.DataFrame(rawd_3, columns = cols).drop("id", axis = 1)

df_sent = perform_sentiment(df = df_sne)
#print(df_sent.columns)

# Check for table: staging.sent_analysis, if not create it 
create_table(schema = SCHEMA_1, table= TABLE_3, 
             query_check = CHECK_TABLE_3, query_create = CREATE_TABLE_3, **db_configs) 

# Insert the processed data into the table
fill_data(df = df_sent, schema = SCHEMA_1, table = TABLE_3, query = fill_query, **db_configs)

# Fetch data that needs to be stored to the app schema
rawd_4, cols = fetch_data(schema = SCHEMA_1, table = TABLE_3, query = fetch_query, query_type = "fetch", **db_configs)
df_ssa = pd.DataFrame(rawd_4, columns = cols).drop("id", axis = 1)
#print("Data for app \n", df_fetch_3.columns)

# Check for table: app.news, if not create it 
create_table(schema = SCHEMA_2, table= TABLE_4, 
             query_check = CHECK_TABLE_4, query_create = CREATE_TABLE_4, **db_configs) 

# Store the data from the staging table to the app table
fill_data(df = df_ssa, schema = SCHEMA_2, table = TABLE_4, query = fill_query, **db_configs)
