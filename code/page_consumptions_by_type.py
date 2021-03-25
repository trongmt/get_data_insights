import json
import facebook as fb
from datetime import datetime
import pandas as pd
from pandas.io.json import json_normalize
import pyodbc
# nhớ change server và database và mở proxies trước khi dùng
page_id='217328504988428'
page_token = 'EAAyBEgkHZCbYBANms9s1kPc3Fwrw3fr9OfZCRDIlJT1hQTfhzlidpUt3irjLqd4EjI4F1KYlEbBkHGm1obIJ1iZC7Hf8da9aU7ZAJsOGCPFlDhUKTM32yr6tJmsPmdhFurmipGis6YxHdQYLdEUZBzuITg1Ynzl6C4w3PzxhJfQZDZD'


def get_values(data):
    devices = []
    for datum in data:
        for i in range(len(datum["values"])):
            id = datum["id"]
            name = datum["name"]
            period = datum["period"]
            title = datum["title"]
            description = datum["description"]
            end_time = datum["values"][i]["end_time"]
            end_time = end_time.split('T')
            end_time = end_time[0]
            value = datum["values"][i]["value"]

            if len(value) >= 1:
                #print(value)
                #print(value['photo view'])

                VideoPlay = value['video play'] if 'video play' in value else 0
                OtherClicks = value['other clicks'] if 'other clicks' in value else 0
                PhotoView = value['photo view'] if 'photo view' in value else 0
                LinkClicks = value['link clicks'] if 'link clicks' in value else 0

                devices.append([id, period, name, end_time, title, description, LinkClicks, OtherClicks, PhotoView, VideoPlay])

            
    return devices

def flatten_json(df):
    flattened_data =[]
    #display(df)
    devices = get_values(df)
    for device in devices:
        id, period, name, end_time, title, description,LinkClicks,OtherClicks,PhotoView,VideoPlay = device
        flattened_data.append([id, period, name, end_time, title, description,LinkClicks,OtherClicks,PhotoView,VideoPlay])
    return flattened_data


def save_to_sql(flat):
    # json_dataframe=pd.DataFrame(flat,columns= ["id", "period", "name", "value", "end_time", "title", "description"])
    # print(type(flat))
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost;'
                      'Database=fb_snp;'
                    #   'username = sa;' 
                    #   'password = admin123$;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    for row in flat:
        # print(row)
        sql = '''insert into dbo.page_consumptions_by_type (id, period, name, end_time, title, description, LinkClicks, OtherClicks, PhotoView, VideoPlay) 
                                            values (?,?,?,?,?,?,?,?,?,?)
            '''
        
        # if isinstance(row[3], dict):
        #     value = ', '.join(row[3].keys())
        # else:
        #     value = row[3]
        insert_news=(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
        cursor.execute(sql, insert_news)
        # cursor.executemany("insert into fb_snp.fb_data[id], [period], [name], [value], [end_time], [title], [description]) values (%('id')s, %('period')s, %('name')s, %('value')s, %('end_time')s,%('title')s,%('description')s",tuple(row))
        conn.commit()

def delete(from_date, to_date):
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost;'
                      'Database=fb_snp;'
                    #   'username = sa;' 
                    #   'password = admin123$;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()                 
    # drop existing records
    sql=f"delete from dbo.page_consumptions_by_type where end_time between '{from_date}' and '{to_date}'"
    # print(sql)
    find = cursor.execute(sql)
    conn.commit()
    return find


if '__name__==__main__':

    # proxies = {
    #     "http": "172.16.0.53:8080",
    #     "https": "172.16.0.53:8080"
    # }

    # graph = fb.GraphAPI(access_token=page_token, version="3.1",  proxies=proxies)
    graph = fb.GraphAPI(access_token=page_token, version="3.1")
    from_date = datetime(2019, 3, 31)
    to_date = datetime(2019, 7, 1)


    # print(from_date)
    page_insights = graph.get_connections(
                        id = page_id,
                        connection_name = 'insights',
                        metric = 'page_consumptions_by_consumption_type',
                        period = 'day',
                        since = from_date,
                        until = to_date,
                        show_description_from_api_doc = False
                )

    # page_cta_clicks_logged_in_unique
    dfs = page_insights['data']
    #print(dfs)
    '''đoạn này chạy nhiều sợ facebook tưởng hack nên cho dô khuôn và thay bằng mở file json'''
    # f = open("e:/github/-Getting-Facebook-Data/code/fb.json",)
    # data=json.load(f)
    # dfs=data["data"]

    delete(from_date, to_date)
    flat=flatten_json(dfs)
    #print(flat)
    save_to_sql(flat)
    #print('OK')

