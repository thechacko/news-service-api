#Create Fast API app
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
import uvicorn
import pandas as pd
import json 
from src.utils.helper import fetch_data
from src.utils.queries import * 
import configparser


# Set up configurations
configs = configparser.ConfigParser()
configs.read(["./configs/database.ini"])
db_configs = {}
db_configs["dbname"] = configs["PostgreSQL"]["dbname"]
db_configs["user"] = configs["PostgreSQL"]["user"]
db_configs["password"] = configs["PostgreSQL"]["password"]
db_configs["host"] = configs["PostgreSQL"]["host"]
db_configs["port"] = configs["PostgreSQL"]["port"]


# Create an item class with user's requirements
#Example:
class Item(BaseModel):
    year: str
    sentiment: Optional[str] = None

# Create a Fast API app
#Example:
app = FastAPI()


@app.get("/")
def hello():
    return {"message": "Hello World"}


# Create a path operation decorator with the path and method
#Example:
@app.post("/items/")
def fetch_item(item: Item): #Blue item is the input the user gives, and the keys are year, product and sentiment
    #create/select the query based on the user's requirements
    request_inputs = {"year_of_pub": "", 
                      "sentiment": ""}
    request_inputs["year_of_pub"] = item.year
    request_inputs["sentiment"] = item.sentiment

    rawd, cols = fetch_data(schema = SCHEMA_2, table = TABLE_1, query = fetch_query, query_type = "request", 
                            request_inputs = request_inputs, **db_configs)
    df = pd.DataFrame(rawd, columns = cols).drop("id", axis = 1)
    df = df[["title", "articles"]]
    #convert dataframe to json/dictionary
    output_dict = df.to_dict()
    return output_dict

# #Run the app
# if __name__ == "__main__":
#     uvicorn.run(app, host="localhost", port=8000)

# #Hit 127.0.0.1:8000/items/ in the browser/postman to send the request as a user


