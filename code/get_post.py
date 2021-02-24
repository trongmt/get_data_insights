import json
import facebook as fb
from datetime import datetime
import pandas as pd
from pandas.io.json import json_normalize
import csv
from IPython.display import display
from sqlalchemy import create_engine
import psycopg2
import sqlalchemy
import calendar
from sqlalchemy.types import String, Integer, Numeric, Float, DateTime
engine = create_engine('postgresql://postgres:postgres@localhost:5432/fb_data')

page_id='217328504988428'
page_token = 'EAAyBEgkHZCbYBANms9s1kPc3Fwrw3fr9OfZCRDIlJT1hQTfhzlidpUt3irjLqd4EjI4F1KYlEbBkHGm1obIJ1iZC7Hf8da9aU7ZAJsOGCPFlDhUKTM32yr6tJmsPmdhFurmipGis6YxHdQYLdEUZBzuITg1Ynzl6C4w3PzxhJfQZDZD'
graph = fb.GraphAPI(access_token=page_token, version="3.1")


def get_values(data):
    devices = []

    for datum in data:
        # for i in range(len(datum["values"])):
            created_time = datum["created_time"]
            message = datum["message"]
            id = datum["id"]
            devices.append([created_time,message,id])
            
    return devices

def flatten_json(df):
    flattened_data =[]
    
    #display(df)
    devices = get_values(df)

    for device in devices:
        created_time,message,id = device
        flattened_data.append([created_time,message,id])
    return flattened_data

def delete(from_date, to_date):
    j_table = sqlalchemy.table("posts")                  
    # drop existing records
    sql=f"delete from posts where created_time between '{from_date}' and '{to_date}'"

    find = engine.execute(sql)
    return find

def data_sql(flat):
    json_dataframe=pd.DataFrame(flat,columns= ["created_time","message","id"])

    json_dataframe.to_sql('posts', engine, if_exists='append', index=False,dtype={"created_time": DateTime(),"message":String(),"id":String()})

if '__name__==__main__':

    from_date = datetime(2020,1,1)
    to_date = datetime(2020,1,15)

    posts = graph.get_connections(         
            id=page_id,
            connection_name="posts",
           # fields="type, name, created_time, object_id", (#12) name field is deprecated for versions v3.3 and higher
            since = from_date,
            until = to_date
    )

dp = posts['data']
delete(from_date,to_date)
p = flatten_json(dp)
data_sql(p)