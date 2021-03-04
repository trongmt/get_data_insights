import json
import facebook as fb
from datetime import datetime
import pandas as pd
from pandas.io.json import json_normalize
#import csv
#from IPython.display import display
#from sqlalchemy import create_engine
import pyodbc
import requests
import sys

page_id='217328504988428'
page_token = 'EAAyBEgkHZCbYBANms9s1kPc3Fwrw3fr9OfZCRDIlJT1hQTfhzlidpUt3irjLqd4EjI4F1KYlEbBkHGm1obIJ1iZC7Hf8da9aU7ZAJsOGCPFlDhUKTM32yr6tJmsPmdhFurmipGis6YxHdQYLdEUZBzuITg1Ynzl6C4w3PzxhJfQZDZD'
# graph = fb.GraphAPI(access_token=page_token, version="3.1")

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
        sql = "insert into dbo.post (date,message,id) values (?,?,?) "
        # if isinstance(row[3], dict):
        #     value = ', '.join(row[3].keys())
        # else:
        #     value = row[3]
        insert_news=(row[0], row[1], row[2])
        cursor.execute(sql, insert_news)
        # cursor.executemany("insert into fb_snp.fb_data[id], [period], [name], [value], [end_time], [title], [description]) values (%('id')s, %('period')s, %('name')s, %('value')s, %('end_time')s,%('title')s,%('description')s",tuple(row))
        conn.commit()

def delete(from_date, to_date):
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=SNP-CNTT-49119;'
                      'Database=fb_snp;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()                 
    # drop existing records
    sql=f"delete from dbo.post where date between '{from_date}' and '{to_date}'"
    # print(sql)
    find = cursor.execute(sql)
    conn.commit()
    return find


if '__name__==__main__':

    from_date = datetime(2021,2,1)
    to_date = datetime(2021,2,28)

    proxies = {
        "http": "172.16.0.53:8080",
        "https": "172.16.0.53:8080"
    }

    #headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Safari/537.36'}
    # js = requests.get('https://graph.facebook.com/v10.0/217328504988428/insights?access_token=EAAyBEgkHZCbYBANms9s1kPc3Fwrw3fr9OfZCRDIlJT1hQTfhzlidpUt3irjLqd4EjI4F1KYlEbBkHGm1obIJ1iZC7Hf8da9aU7ZAJsOGCPFlDhUKTM32yr6tJmsPmdhFurmipGis6YxHdQYLdEUZBzuITg1Ynzl6C4w3PzxhJfQZDZD&period=day&metric=page_views_total,page_post_engagements,page_fans,page_fan_adds_unique', proxies=proxies, headers=headers, verify=False)
    # print(js.status_code)
    # print(js.text)

    print(from_date)
    graph = fb.GraphAPI(access_token=page_token, version="3.1",  proxies=proxies)

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
save_to_sql(p)