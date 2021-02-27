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
        if len(datum) > 2:
            if 'message' in datum:
                created_time = datum["created_time"]
                created_time = created_time.split('T')
                date = created_time[0]
                message = datum['message']
                id = datum["id"]
                devices.append([date,message,id])
            
    return devices

def flatten_json(df):
    flattened_data =[]
    
    #display(df)
    devices = get_values(df)

    for device in devices:
        date,message,id = device
        flattened_data.append([date,message,id])
    return flattened_data

def delete(from_date, to_date):
    j_table = sqlalchemy.table("posts")                  
    # drop existing records
    sql=f"delete from posts where date between '{from_date}' and '{to_date}'"

    find = engine.execute(sql)
    return find

def data_sql(flat):
    json_dataframe=pd.DataFrame(flat,columns= ["date","message","id"])

    json_dataframe.to_sql('posts', engine, if_exists='append', index=False,dtype={"date": DateTime(),"message":String(),"id":String()})

# def get_post(year,month):
#     last_day_in_month = calendar.monthrange(year, month)[1]    
#     return  graph.get_connections(         
#             id=page_id,
#             connection_name="posts",
#            # fields="type, name, created_time, object_id", (#12) name field is deprecated for versions v3.3 and higher
#             since=datetime(year, month, 1, 0, 0, 0),
#             until=datetime(year, month, last_day_in_month, 0, 0, 0),
#             show_description_from_api_doc = False

if '__name__==__main__':

    from_date = datetime(2021,1,1)
    to_date = datetime(2021,1,31)

    posts = graph.get_connections(         
            id=page_id,
            connection_name="posts",
           # fields="type, name, created_time, object_id", (#12) name field is deprecated for versions v3.3 and higher
            since = from_date,
            until = to_date,
            show_description_from_api_doc = False
    )

dp = posts['data']
delete(from_date,to_date)
p = flatten_json(dp)
data_sql(p)