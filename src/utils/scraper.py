from loguru import logger
from datetime import datetime
import requests
import os
from tqdm import tqdm
from bs4 import BeautifulSoup as bs
import pandas as pd
import time

today = datetime.today().strftime("%Y-%m-%d")

# Function to get response from the url
def fetch_url_response(url):
    try:
        # Get response from the url
        response = requests.get(url)
        logger.info("Fetched the response from the url")

        return response
    except Exception as e:
        logger.error(f"Error in fetching the response from the url: {e}")

        return None


# Function to parse and save raw html from the response
def save_rawd(response): 
    try:
        with open(f"../data/livemint_{today}.txt", "wb") as f:
            f.write(response.content)
        logger.info("Raw data has been saved")
    except Exception as e:
        logger.error(f"Error in saving the raw data: {e}")


# Function to get the data from the raw html
def fetch_fix_rawd(directory): 
    try:
        for file in os.listdir(directory):
            path = os.path.join(directory, file)

            if ("livemint" in path):
                item_date = path.split("/")[-1].split("_")[-1].split(".")[0]
                time_diff = datetime.today() - datetime.strptime(item_date, '%Y-%m-%d')
        
                if(time_diff.days > 7): 
                    os.remove(path)
                    logger.info("Files that are older than 7 days are removed")
                else:
                    raise FileNotFoundError("Could not find data older than 7 days")
    except Exception as e: 
        logger.error(f"{e}")

    try:
        with open(f'../data/livemint_{today}.txt', 'rb') as f:
            loaded_content = f.read()
        logger.info("Raw Data has been loaded")

        return loaded_content  
    except Exception as e: 
        logger.error(f"Could not load the raw data: {e}")

# Function to process the data retrieved from the raw html
"""def scrape_process(loaded_content): ## Works on jupyter notebook, not otherwise
    loaded_soup = bs(loaded_content, features='xml')
    urls = loaded_soup.find_all('url')

    yday_news = []

    for u in urls:
        art_data = {}

        try:
            art_data['news_link'] = u.find('loc').text
        except Exception:
            art_data['news_link'] = None

        try:
            art_data['pub_date'] = u.find('news:publication_date').text
        except Exception:
            art_data['pub_date'] = None

        try:
            art_data['title'] = u.find('news:title').text
        except Exception:
            art_data['title'] = None

        try:
            art_data['keywords'] = u.find('news:keywords').text
        except Exception:
            art_data['keywords'] = None
        
        yday_news.append(art_data)

    try:
        loaded_soup = bs(loaded_content, features='xml')
        urls = loaded_soup.find_all('url')

        yday_news = []

        for u in urls:
            art_data = {}

            try:
                art_data['news_link'] = u.find('loc').text
            except Exception:
                art_data['news_link'] = None

            try:
                art_data['pub_date'] = u.find('news:publication_date').text
            except Exception:
                art_data['pub_date'] = None

            try:
                art_data['title'] = u.find('news:title').text
            except Exception:
                art_data['title'] = None

            try:
                art_data['keywords'] = u.find('news:keywords').text
            except Exception:
                art_data['keywords'] = None
            
            yday_news.append(art_data)

        df = pd.json_normalize(yday_news)
        df.insert(2, 'category', df['news_link'].apply(lambda x: x.split("/")[3]))
        df = df[df['category']=='companies']
        df.drop('category', axis=1, inplace=True)

        try:
            article_text = []
            
            for link in tqdm(df['news_link']):
                time.sleep(1)
                r = requests.get(link)
                soup = bs(r.text, 'html.parser')
                article_text.append(" ".join([item.text for item in soup.find_all('p')]))

            df['articles'] = article_text
        except Exception:
            df['articles'] = ""

        df['scraped_date'] = today
        logger.info("Scraping successfully completed")
    except Exception as e:
        logger.error(f"Unable to scrape data: {e}")
        
    return df """
    

def scrape_process(loaded_content):
    loaded_soup = bs(loaded_content, features='xml')
    urls = loaded_soup.find_all('url')

    yday_news = []

    for u in urls:
        art_data = {}

        try:
            art_data['news_link'] = u.find('loc').text
        except Exception:
            art_data['news_link'] = None

        try:
            art_data['pub_date'] = u.find('news:publication_date').text
        except Exception:
            art_data['pub_date'] = None

        try:
            art_data['title'] = u.find('news:title').text
        except Exception:
            art_data['title'] = None

        try:
            art_data['keywords'] = u.find('news:keywords').text
        except Exception:
            art_data['keywords'] = None

        yday_news.append(art_data)

    df = pd.json_normalize(yday_news)
    df.insert(2, 'category', df['news_link'].apply(lambda x: x.split("/")[3]))
    df = df[df['category'] == 'companies']
    df.drop('category', axis=1, inplace=True)

    article_text = []

    for link in tqdm(df['news_link']):
        time.sleep(1)
        try:
            r = requests.get(link)
            soup = bs(r.text, 'html.parser')
            article_text.append(" ".join([item.text for item in soup.find_all('p')]))
        except Exception:
            article_text.append("")

    df['articles'] = article_text
    df['scraped_date'] = today
    logger.info("Scraping successfully completed")

    return df

        