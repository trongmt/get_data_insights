import json
import facebook as fb
from datetime import datetime
import pandas as pd
from pandas.io.json import json_normalize
#import csv
#from IPython.display import display
#from sqlalchemy import create_engine
import pyodbc
import calendar
import sys
import requests

page_id='217328504988428'
page_token = 'EAAyBEgkHZCbYBANms9s1kPc3Fwrw3fr9OfZCRDIlJT1hQTfhzlidpUt3irjLqd4EjI4F1KYlEbBkHGm1obIJ1iZC7Hf8da9aU7ZAJsOGCPFlDhUKTM32yr6tJmsPmdhFurmipGis6YxHdQYLdEUZBzuITg1Ynzl6C4w3PzxhJfQZDZD'
# graph = fb.GraphAPI(access_token=page_token, version="3.1")

def get_post(year,month):
    last_day_in_month = calendar.monthrange(year, month)[1]
    #print(last_day_in_month)
    return  graph.get_connections(         
            id=page_id,
            connection_name="posts",
           # fields="type, name, created_time, object_id", (#12) name field is deprecated for versions v3.3 and higher
            since=datetime(year, month, 1, 0, 0, 0),
            until=datetime(year, month, last_day_in_month, 23, 59, 59),
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
                devices.append([created_time,message,id])
        
    return devices

def flatten_json_post(df):
    flattened_data =[]
    
    #display(df)
    devices = get_values_post(df)

    for device in devices:
        #print(device)
        created_time,message,id = device
        flattened_data.append([created_time,message,id])
    return flattened_data

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
                      'Server=SNP-CNTT-49119;'
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
            post_reactions_like_total,
            post_reactions_love_total,
            post_reactions_wow_total,
            post_reactions_haha_total,
            post_reactions_sorry_total,
            post_reactions_anger_total,

            ''',
            # metric = '''
            
            
            # ''',
            period="lifetime",
            show_description_from_api_doc=False,
        )

if '__name__==__main__':
    proxies = {
        "http": "172.16.0.53:8080",
        "https": "172.16.0.53:8080"
    }

    #headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Safari/537.36'}
    # js = requests.get('https://graph.facebook.com/v10.0/217328504988428/insights?access_token=EAAyBEgkHZCbYBANms9s1kPc3Fwrw3fr9OfZCRDIlJT1hQTfhzlidpUt3irjLqd4EjI4F1KYlEbBkHGm1obIJ1iZC7Hf8da9aU7ZAJsOGCPFlDhUKTM32yr6tJmsPmdhFurmipGis6YxHdQYLdEUZBzuITg1Ynzl6C4w3PzxhJfQZDZD&period=day&metric=page_views_total,page_post_engagements,page_fans,page_fan_adds_unique', proxies=proxies, headers=headers, verify=False)
    # print(js.status_code)
    # print(js.text)

    # print(from_date)
    graph = fb.GraphAPI(access_token=page_token, version="3.1",  proxies=proxies)

    posts=get_post(2020,12)
    p = flatten_json_post(posts['data'])
    # print(posts)
    for i in range(len(p)):
            post_id = p[i][2]
            # print(post_id)
            post_insight = get_post_insights(post_id)
            dfs = post_insight['data']
            # print(dfs)
            flat=flatten_json(dfs)
            save_to_sql(flat)