import json
import facebook as fb
from datetime import datetime
import pandas as pd
from pandas.io.json import json_normalize
import pyodbc
import calendar
# nhớ change server và database và mở proxies trước khi dùng
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
                      'Server=localhost;'
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


def delete(year,month):
    last_day_in_month = calendar.monthrange(year, month)[1]
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost;'
                      'Database=fb_snp;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    # sql=f"delete from dbo.post where date between '{from_date}' and '{to_date}'"
    sql=f"delete from dbo.post where year(date)='{year}' and month(date)='{month}'"
    # print(sql)
    find = cursor.execute(sql)   
    conn.commit()
    return find
# Co cach khac la set tren database cho id , date ko trung nhau thi minh khong phai lo van de chay lap thang do

if '__name__==__main__':
    # proxies = {
    #     "http": "172.16.0.53:8080",
    #     "https": "172.16.0.53:8080"
    # }

    # print(from_date)
    # graph = fb.GraphAPI(access_token=page_token, version="3.1",  proxies=proxies)
    graph = fb.GraphAPI(access_token=page_token, version="3.1")
    year = 2020
    month = 10
    dp=get_post(year,month)
# dp = posts['data']
    delete(year,month)
    p = flatten_json(dp['data'])
    save_to_sql(p)