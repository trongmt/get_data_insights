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

# from_date = datetime(2021,1,1)
# to_date = datetime(2021,1,15)
def get_post(year,month):
    last_day_in_month = calendar.monthrange(year, month)[1]    
    return  graph.get_connections(         
            id=page_id,
            connection_name="posts",
           # fields="type, name, created_time, object_id", (#12) name field is deprecated for versions v3.3 and higher
            since=datetime(year, month, 1, 0, 0, 0),
            until=datetime(year, month, last_day_in_month, 0, 0, 0),
            show_description_from_api_doc = False
    )
def get_values_post(data):
    devices = []

    for datum in data:
        if len(datum) > 2:
            if 'message' in datum:
                created_time = datum["created_time"]
                message = datum['message']
                id = datum["id"]
                # id = id.split('_')
                # id_page = id[0]
                # id_post = id[1]
        devices.append([created_time,message,id])
        
    return devices

def flatten_json_post(df):
    flattened_data =[]
    
    #display(df)
    devices = get_values_post(df)

    for device in devices:
        created_time,message,id = device
        flattened_data.append([created_time,message,id])
    return flattened_data

def get_values(data):
    devices = []

    for datum in data:
        for i in range(len(datum["values"])):
            id = datum["id"]
            name = datum["name"]
            period = datum["period"]
            title = datum["title"]
            description = datum["description"]
            value = datum["values"][i]["value"]
            # end_time = datum["values"][i]["end_time"]
            devices.append([id, period, name, value, title, description])
            
    return devices

def flatten_json(df):
    flattened_data =[]
    
    #display(df)
    devices = get_values(df)

    for device in devices:
        id, period, name, value, title, description = device
        flattened_data.append([id, period, name, value, title, description])
    return flattened_data

def data_sql(flat):
    json_dataframe=pd.DataFrame(flat,columns= ["id", "period", "name", "value", "title", "description"])

    json_dataframe.to_sql('post_insight', engine, if_exists='append', index=False,dtype={"id": String(),  "period":String(), "name":String(), "value":Integer(),"title":String(), "description":String()})

# def delete(from_date, to_date):
#     j_table = sqlalchemy.table("post_insight")                  
#     # drop existing records
#     sql=f"delete from post_insight 
#           where end_time between '{from_date}' and '{to_date}'"

#     find = engine.execute(sql)
#     return find

def get_post_insights(post_id):
    return graph.get_connections(
            id=post_id,
            connection_name="insights",
            metric='''post_engaged_users,post_negative_feedback,post_negative_feedback_unique,post_engaged_fan,post_clicks,post_clicks_unique,
            post_impressions,post_impressions_unique,post_impressions_paid,
            post_impressions_paid_unique,post_impressions_fan,post_impressions_fan_unique,
            post_impressions_fan_paid,post_impressions_fan_paid_unique,post_impressions_organic,post_impressions_organic_unique,
            post_video_avg_time_watched,post_video_complete_views_organic,post_video_complete_views_organic_unique,
            post_video_complete_views_paid,post_video_complete_views_paid_unique,
            post_video_views_organic,post_video_views_organic_unique,
            post_video_views_paid,post_video_views_paid_unique,
            post_video_length,post_video_views,post_video_views_unique
            ''',
            period="lifetime",
            show_description_from_api_doc=False,
        )

if '__name__==__main__':

    posts=get_post(2019,2)
    p = flatten_json_post(posts['data'])
    # print(type(p))
    for i in range(len(p)):
            post_id = p[i][2]
            # print(post_id)
            post_insight = get_post_insights(post_id)
            dfs = post_insight['data']
            flat=flatten_json(dfs)
            data_sql(flat)


# print(dfs)


# print(type(flat))
