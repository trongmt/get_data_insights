import json
import facebook as fb
from datetime import datetime
import pandas as pd
from pandas.io.json import json_normalize
import pyodbc
import calendar
import sys
import requests

page_id='217328504988428'
page_token = 'EAAyBEgkHZCbYBANms9s1kPc3Fwrw3fr9OfZCRDIlJT1hQTfhzlidpUt3irjLqd4EjI4F1KYlEbBkHGm1obIJ1iZC7Hf8da9aU7ZAJsOGCPFlDhUKTM32yr6tJmsPmdhFurmipGis6YxHdQYLdEUZBzuITg1Ynzl6C4w3PzxhJfQZDZD'
# graph = fb.GraphAPI(access_token=page_token, version="3.1")

def get_values(data):
    devices = []

    for datum in data:
        for i in range(len(datum["values"])):
            id = datum["id"]
            id = id.split('/')
            id = id[0]
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


def save_to_sql(flat):
    # json_dataframe=pd.DataFrame(flat,columns= ["id", "period", "name", "value", "end_time", "title", "description"])
    # print(type(flat))
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost;'
                      'Database=fb_snp;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    for row in flat:
        # print(row)
        sql = "insert into dbo.post_insight (id, period, name, value, title, description) values (?,?,?,?,?,?) "
        # if isinstance(row[3], dict):
        #     value = ', '.join(row[3].keys())
        # else:
        #     value = row[3]
        insert_news=(row[0], row[1], row[2], row[3], row[4], row[5])
        cursor.execute(sql, insert_news)
        # cursor.executemany("insert into fb_snp.fb_data[id], [period], [name], [value], [end_time], [title], [description]) values (%('id')s, %('period')s, %('name')s, %('value')s, %('end_time')s,%('title')s,%('description')s",tuple(row))
        conn.commit()


def get_post_insights(post_id):
    return graph.get_connections(
            id=post_id,
            connection_name="insights",
            metric='''
            post_engaged_users,
            post_engaged_fan,post_clicks,post_clicks_unique,
            post_impressions,post_impressions_unique,post_impressions_paid,
            post_impressions_paid_unique,post_impressions_fan,post_impressions_fan_unique,
            post_activity
            ''',
            period="lifetime",
            show_description_from_api_doc=False,
        )

if '__name__==__main__':
#     proxies = {
#         "http": "172.16.0.53:8080",
#         "https": "172.16.0.53:8080"
#     }

    # graph = fb.GraphAPI(access_token=page_token, version="3.1",  proxies=proxies)
    graph = fb.GraphAPI(access_token=page_token, version="3.1") 
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost;'
                      'Database=fb_snp;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    cursor.execute("select posts.id from post left join post_insight on posts.id = post_insight.id where post_insight.id is null and posts.date between '2020-07-01' and '2020-12-31' order by date desc")
    lstPostIds = cursor.fetchall()

    for postId in lstPostIds:
        post_insight = get_post_insights(postId[0])
        dfs = post_insight['data']
        flat = flatten_json(dfs)
        save_to_sql(flat)

