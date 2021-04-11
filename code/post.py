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


def get_values(data):
    devices = []

    created_time = data["created_time"] if 'created_time' in data else ''
    created_time = created_time.split('T')
    date = created_time[0]
    message = data["message"] if 'message' in data else ''
    id = data["id"] if 'id' in data else ''
    story = data["story"] if 'story' in data else ''

    devices.append([date,message,story,id])

    return devices

def flatten_json(df):
    flattened_data =[]
    
    #display(df)
    devices = get_values(df)

    for device in devices:
        date,message,story,id = device
        # print(device)
        flattened_data.append([date,message,story,id])
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
        sql = "insert into dbo.post (date,message,story,id) values (?,?,?,?) "
        insert_news=(row[0], row[1], row[2], row[3])
        cursor.execute(sql, insert_news)
        # cursor.executemany("insert into fb_snp.fb_data[id], [period], [name], [value], [end_time], [title], [description]) values (%('id')s, %('period')s, %('name')s, %('value')s, %('end_time')s,%('title')s,%('description')s",tuple(row))
        conn.commit()

# Co cach khac la set constraint tren database cho id ko trung nhau thi minh khong phai lo van de chay lap thang do

if '__name__==__main__':
    # proxies = {
    #     "http": "172.16.0.53:8080",
    #     "https": "172.16.0.53:8080"
    # }

    # print(from_date)
    # graph = fb.GraphAPI(access_token=page_token, version="3.1",  proxies=proxies)
    graph = fb.GraphAPI(access_token=page_token, version="3.1")
    posts = graph.get_all_connections(id=page_id,
                                        connection_name='posts',
                                        # fields='type, name, created_time, object_id',
                                       since=datetime(2019, 1, 1))
                                    #    until=datetime(2019,4,1))
    # print(posts)
    for ind, post in enumerate(posts):
        p = flatten_json(post)
        # print(p)
        save_to_sql(p)