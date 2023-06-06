# SQL Query to create table in the database
SCHEMA_1 = "staging"
TABLE_1 = "news"
TABLE_2 = "ner"
TABLE_3 = "sent_analysis"

SCHEMA_2 = "app"
TABLE_4 = "news" 

SCHEMA_TABLE = {"staging": ["news", "ner", "sent_analysis"], 
                "app": "news"}


CHECK_TABLE_1 = f"""
                SELECT EXISTS 
                    (
                      SELECT 
                      FROM pg_tables 
                      WHERE schemaname = '{SCHEMA_1}' AND tablename = '{TABLE_1}'
                    );
                """
CHECK_TABLE_2 = f"""
                SELECT EXISTS 
                    (
                      SELECT 
                      FROM pg_tables 
                      WHERE schemaname = '{SCHEMA_1}' AND tablename = '{TABLE_2}'
                    );
                """

CHECK_TABLE_3 = f"""
                SELECT EXISTS 
                    (
                      SELECT 
                      FROM pg_tables 
                      WHERE schemaname = '{SCHEMA_1}' AND tablename = '{TABLE_3}'
                    );
                """

CHECK_TABLE_4 = f"""
                SELECT EXISTS 
                    (
                      SELECT 
                      FROM pg_tables 
                      WHERE schemaname = '{SCHEMA_2}' AND tablename = '{TABLE_4}'
                    );
                """                

CREATE_TABLE_1 = f"""
                  CREATE TABLE {SCHEMA_1}.{TABLE_1}
                    (
                      id serial PRIMARY KEY,
                      news_link TEXT,
                      pub_date TEXT,
                      title TEXT,
                      keywords TEXT,
                      articles TEXT,
                      scraped_date TEXT
                    );
                """   
CREATE_TABLE_2 = f"""
                  CREATE TABLE {SCHEMA_1}.{TABLE_2}
                    (
                      id serial PRIMARY KEY,
                      year_of_pub INT, 
                      month_of_pub TEXT, 
                      day_pub TEXT, 
                      time_of_day TEXT,
                      title TEXT, 
                      articles TEXT, 
                      keywords TEXT, 
                      org TEXT[], 
                      location TEXT[], 
                      product TEXT[], 
                      news_link TEXT,
                      scraped_date TEXT
                    );
                """   
                
CREATE_TABLE_3 = f"""
                  CREATE TABLE {SCHEMA_1}.{TABLE_3}
                    (
                      id serial PRIMARY KEY,
                      year_of_pub INT, 
                      month_of_pub TEXT, 
                      day_pub TEXT, 
                      time_of_day TEXT,
                      title TEXT, 
                      articles TEXT, 
                      keywords TEXT, 
                      org TEXT[], 
                      location TEXT[], 
                      product TEXT[], 
                      sentiment TEXT,
                      news_link TEXT,
                      scraped_date TEXT
                    );
                """ 

CREATE_TABLE_4 = f"""
                  CREATE TABLE {SCHEMA_2}.{TABLE_4}
                    (
                      id serial PRIMARY KEY,
                      year_of_pub INT, 
                      month_of_pub TEXT, 
                      day_pub TEXT, 
                      time_of_day TEXT,
                      title TEXT, 
                      articles TEXT, 
                      keywords TEXT[], 
                      org TEXT[], 
                      location TEXT[], 
                      product TEXT[], 
                      sentiment TEXT,
                      news_link TEXT,
                      scraped_date TEXT
                    );
                """ 

# SQL Query to add the data to the database
fill_query = """INSERT INTO table_name ({','.join(columns)}) VALUES %s"""

# SQL Queries to fetch the data from the database
fetch_query = f"""select * 
                  from table_name 
                """

