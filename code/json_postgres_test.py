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
from sqlalchemy.types import String, Integer, Numeric, Float, DateTime
engine = create_engine('postgresql://postgres:postgres@localhost:5432/fb_data')

page_id='217328504988428'
page_token = 'EAAyBEgkHZCbYBANms9s1kPc3Fwrw3fr9OfZCRDIlJT1hQTfhzlidpUt3irjLqd4EjI4F1KYlEbBkHGm1obIJ1iZC7Hf8da9aU7ZAJsOGCPFlDhUKTM32yr6tJmsPmdhFurmipGis6YxHdQYLdEUZBzuITg1Ynzl6C4w3PzxhJfQZDZD'
graph = fb.GraphAPI(access_token=page_token, version="3.1")

j_table = sqlalchemy.table("fb_data")                  
    # drop existing records
    # sql=f"delete from fb_data where end_time between '{from_date}' and '{to_date}'"

find = engine.execute(j_table.delete())

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
            end_time = datum["values"][i]["end_time"]
            devices.append([id, period, name, value, end_time, title, description])
            
    return devices

def flatten_json(df):
    flattened_data =[]
    
    #display(df)
    devices = get_values(df)

    for device in devices:
        id, period, name, value, end_time, title, description = device
        flattened_data.append([id, period, name, value, end_time, title, description])
    return flattened_data

def data_sql(flat):
    json_dataframe=pd.DataFrame(flat,columns= ["id", "period", "name", "value", "end_time", "title", "description"])

    json_dataframe.to_sql('fb_data', engine, if_exists='append', index=False,dtype={"id": String(),  "period":String(), "name":String(), "value":Integer(), "end_time":DateTime(), "title":String(), "description":String()})

def delete(from_date, to_date):
    j_table = sqlalchemy.table("fb_data")                  
    # drop existing records
    sql=f"delete from fb_data where end_time between '{from_date}' and '{to_date}'"

    find = engine.execute(sql)
    return find

if '__name__==__main__':
    from_date = datetime(2018, 2, 31)
    to_date = datetime(2019, 3, 1)
    page_insights = graph.get_connections(
                        id = page_id,
                        connection_name = 'insights',
                        metric = '''page_fans,page_fan_adds_unique,page_fan_removes_unique,
                                    page_engaged_users,page_impressions_unique,page_impressions_organic_unique,
                                    page_impressions_paid_unique,page_impressions_viral_unique,page_impressions,
                                    page_impressions_organic,page_impressions_paid,page_impressions_viral,
                                    page_posts_impressions_unique,page_posts_impressions_organic_unique,page_posts_impressions_paid_unique,
                                    page_posts_impressions_viral_unique,page_posts_impressions,page_posts_impressions_organic,
                                    page_posts_impressions_paid,page_posts_impressions_viral,page_consumptions_unique,
                                    page_consumptions,page_negative_feedback_unique,page_negative_feedback,
                                    page_places_checkin_total,page_places_checkin_total_unique,page_places_checkin_mobile,
                                    page_places_checkin_mobile_unique,page_video_views_organic,page_video_views_paid,
                                    page_video_complete_views_30s_organic,page_video_complete_views_30s_paid,page_video_views,
                                    page_video_repeat_views,page_video_views_unique,page_video_complete_views_30s,page_video_complete_views_30s_autoplayed,
                                    page_video_complete_views_30s_click_to_play,page_video_complete_views_30s_repeat_views,page_video_complete_views_30s_unique,
                                    page_total_actions,page_get_directions_clicks_logged_in_unique,page_call_phone_clicks_logged_in_unique,page_website_clicks_logged_in_unique
                                ''',
                        period = 'day',
                        since = from_date,
                        until = to_date,
                        show_description_from_api_doc = False
                )
    dfs = page_insights['data']
    '''đoạn này chạy nhiều sợ facebook tưởng hack nên cho dô khuôn và thay bằng mở file json'''
    # f = open("e:/github/-Getting-Facebook-Data/code/fb.json",)
    # data=json.load(f)
    # dfs=data["data"]

    delete(from_date, to_date)
    flat=flatten_json(dfs)
    data_sql(flat)

