from loguru import logger
import psycopg2
from psycopg2.extras import execute_values
import configparser
from datetime import datetime
import re 
import pandas as pd
import numpy as np
import spacy
import en_core_web_lg
from textblob import TextBlob

# Function to check for database connection
def check_conn(**params):
    dbname = params["dbname"]

    try:
        with psycopg2.connect(**params) as conn:
            logger.info(f"Able to connect to the database: '{dbname}'")
    except Exception as e:
        logger.info(f"Unable to connect to the database: {e}")

# Function to check for table
def create_table(schema:str=None, table:str=None, query_check:str=None, query_create:str=None, **params):
    try:
        with psycopg2.connect(**params) as conn:

            with conn.cursor() as cur:
                check_table = query_check.format(schema, table)
                cur.execute(check_table)

                if bool(cur.fetchone()[0]):
                    logger.info(f"Table '{schema}.{table}' exists. Moving On")
                    conn.commit()
                else:
                    logger.info(f"Table does not exist. Creating table: '{schema}.{table}'")
                    cur.execute(query_create)
                    conn.commit()
                    logger.info(f"Table '{schema}.{table}' created")
    except Exception as e:
        logger.error(f"Unable to connect to table: {e}")

# Function to fetch the data from the table
def fetch_data(schema, table, query, query_type, request_inputs = {}, **params):
    table_name = f'{schema}.{table}'
    

    if(query_type == "fetch"):
        query = query.replace("table_name", table_name)
    elif(query_type == "request"): 
        slices = ["Where"]
        print("Going to enter Loop")
        for key, value in request_inputs.items():
            print("Entered For Loop")
        
            if(value.isdigit()): 
                slices.append(f"{key} = {value}")
            elif(value != ""):
                slices.append(f"AND {key} = '{value}'")
            elif(value == ""):
                slices.append(f"AND {key} IS NOT NULL")

        filters = ",".join(slices).replace(",", " ")
        query = query.replace("table_name", table_name)
        query = query + filters

    try:
        with psycopg2.connect(**params) as conn:

            with conn.cursor() as cur:
                cur.execute(query)
                cols = [desc[0] for desc in cur.description]
                rawd = cur.fetchall()
                conn.commit()
                logger.info(f"Data has been fetched from '{table_name}'")

                return rawd, cols 
    except Exception as e: 
        logger.info(f"Data could be fetched from '{table_name}'")
    
# Function to insert the processed data into the table
def fill_data(df, schema, table, query, **params):
    table_name = f'{schema}.{table}'
    columns = df.columns
    values = [tuple(row) for row in df[columns].values]
    query = query.replace("table_name", table_name).replace("{','.join(columns)}", ",".join(columns))

    try:
        with psycopg2.connect(**params) as conn:

	        with conn.cursor() as cur:
		        cur.execute(f"TRUNCATE TABLE {table_name}") #this line should apply only the first time, this removes all previously stored data
		        #query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES %s"
		        execute_values(cur, query, values)
		        conn.commit()
        logger.info(f"Able to fill data into the table: '{table_name}'")
    except Exception as e: 
        logger.info(f"Unable to fill to the table: {e}")
    
# Function to process and feature engineer the data
def process_feature(df): 
    for i in range(len(df)): 
        df['title'][i] = re.sub(' +', ' ', df['title'][i])
        df['keywords'][i] = df['keywords'][i].lower()
        df['articles'][i] = re.sub(' +', ' ', df['articles'][i])
        df['articles'][i] = re.sub(r'\b(\w+)\s+([^\w\s])', r'\1\2', df['articles'][i])

    df['pub_date'] = pd.to_datetime(df['pub_date'])
    df['time_of_day'] = 'str'
    df['year_of_pub'] = df['pub_date'].dt.year
    df['month_of_pub'] = df['pub_date'].dt.month_name(locale = 'English').str.lower()
    df['day_pub'] = df['pub_date'].dt.day_name().str.lower() 

    for i in range(len(df)): 
        date_time = df['pub_date'][i]

        if(date_time.hour < 12): 
            df.loc[i, 'time_of_day'] = 'morning'
        elif(date_time.hour < 16): 
            df.loc[i, 'time_of_day'] = 'afternoon'
        elif(date_time.hour < 18): 
            df.loc[i, 'time_of_day'] = 'evening'
        else: 
            df.loc[i, 'time_of_day'] = 'night'       

    feats = ['pub_date', 'year_of_pub', 'month_of_pub', 'day_pub', 'time_of_day', 'title', 'keywords', 'articles']
    ref_feats = ['news_link', 'scraped_date']
    logger.info("Data has been cleaned and some features added: Ready for Ner")

    return df[feats + ref_feats]

# Function to perform NER
def perform_ner(df):
    ner = en_core_web_lg.load()

    df['ner_data'] = df['title'] + '. ' + df['articles']
    
    df['keyword_entity_maps'] = {}
    df['entity_maps'] = {} 

    for i in range(len(df)): 
        text_doc = ner(df['keywords'][i])
        keyword_entity_maps = {}

        for word in text_doc.ents: 

            if word.label_ in keyword_entity_maps: 

                if word.text not in keyword_entity_maps[word.label_]: 
                    keyword_entity_maps[word.label_].append(word.text)
            else: 
                keyword_entity_maps[word.label_] = [word.text]

        df['keyword_entity_maps'][i] = keyword_entity_maps

        text_doc = ner(df['ner_data'][i])
        entity_maps = {} 
        
        for word in text_doc.ents:
            
            if word.label_ in entity_maps: 

                if word.text not in entity_maps[word.label_]:
                    entity_maps[word.label_].append(word.text)
            else: 
                entity_maps[word.label_] = [word.text]
 
        df['entity_maps'][i] = entity_maps

    df = pd.concat([df, df['entity_maps'].apply(lambda x: pd.Series({f'em_{key}': value for key, value in x.items()}))], axis = 1)
    df = pd.concat([df, df['keyword_entity_maps'].apply(lambda x: pd.Series({f'kem_{key}': value for key, value in x.items()}))], axis = 1)
    df["keywords"] = df["keywords"].apply(lambda x: x.split(","))

    feats = ['pub_date', 'year_of_pub', 'month_of_pub', 'day_pub', 'time_of_day', 'title', 'articles', 'keywords']
    ner_feats = [col for col in df.columns if 'ner' in col or 'entity' in col or 'em_' in col or 'kem_' in col]
    ner_feats = [feat.lower() for feat in ner_feats]
    ref_feats = ['news_link', 'scraped_date']
    cols = [col.lower() for col in df.columns]
    df.columns = cols

    for feat in ner_feats: 
        df.loc[df[feat].isnull(),feat] = df.loc[df[feat].isnull(),feat].apply(lambda x: [])

    select_ner_feats = ["em_org", "kem_org", "em_gpe", "kem_gpe", "em_product", "kem_product"]

    for feat in select_ner_feats:
    
        if feat not in df.columns:
            df[feat] = np.empty((len(df), 0)).tolist()    

    df['org'] = df['em_org'] + df['kem_org']
    df['location'] = df['em_gpe'] + df['kem_gpe']
    df['product'] = df['em_product'] + df['kem_product']

    ner_feats2 = ['org', 'location', 'product']

    df = df[feats + ner_feats2 + ref_feats]

    df.loc[df['org'].isnull(), 'org'] = df.loc[df['org'].isnull(), 'org'].apply(lambda x: [])
    df.loc[df['location'].isnull(), 'location'] = df.loc[df['location'].isnull(), 'location'].apply(lambda x: [])
    df.loc[df['product'].isnull(), 'product'] = df.loc[df['product'].isnull(), 'product'].apply(lambda x: [])
    df['org'] = df['org'].apply(lambda x: [i.lower() for i in x])
    df['location'] = df['location'].apply(lambda x: [i.lower() for i in x])
    df['product'] = df['product'].apply(lambda x: [i.lower() for i in x])
    
    select_feats = ['year_of_pub', 'month_of_pub', 'day_pub', 'time_of_day', 'title', 'articles', 'keywords', 'org', 'location', 'product', 
                    'news_link', 'scraped_date']

    logger.info("Ner has been performed")

    return df[select_feats]

# Function to perform sentiment analysis
def perform_sentiment(df):
    df['polarity'] = df['title'].apply(lambda x: TextBlob(x).sentiment.polarity)
    df['sentiment'] = np.where(df['polarity'] < -0.1, "Negative", np.where(df['polarity'] > 0.1, 'Positive', 'Neutral'))

    select_feats = ['year_of_pub', 'month_of_pub', 'day_pub', 'time_of_day', 'title', 'articles', 
                    'keywords', 'org', 'location', 'product', 'sentiment', 'news_link', 'scraped_date']

    logger.info("Sentiment of articles added")

    return df[select_feats]














