import json
import facebook as fb
from datetime import datetime
import pandas as pd
from pandas.io.json import json_normalize
import pyodbc
import calendar

page_id='217328504988428'
page_token = 'EAAyBEgkHZCbYBANms9s1kPc3Fwrw3fr9OfZCRDIlJT1hQTfhzlidpUt3irjLqd4EjI4F1KYlEbBkHGm1obIJ1iZC7Hf8da9aU7ZAJsOGCPFlDhUKTM32yr6tJmsPmdhFurmipGis6YxHdQYLdEUZBzuITg1Ynzl6C4w3PzxhJfQZDZD'

def get_values(data):
    devices = []

    #print(data)
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
            if len(value) >= 0:
                VideoPlay = value['video play'] if 'video play' in value else 0
                OtherClicks = value['other clicks'] if 'other clicks' in value else 0
                PhotoView = value['photo view'] if 'photo view' in value else 0
                LinkClicks = value['link clicks'] if 'link clicks' in value else 0

                devices.append([id, period, name, title, description, LinkClicks, OtherClicks, PhotoView, VideoPlay])
            
    return devices

def flatten_json(df,orgPostid):
    flattened_data =[]
    
    #display(df)
    devices = get_values(df)

    for device in devices:
        id, period, name, title, description, LinkClicks, OtherClicks, PhotoView, VideoPlay = device
        flattened_data.append([id, period, name, title, description, LinkClicks, OtherClicks, PhotoView, VideoPlay, orgPostid])
    return flattened_data


# def save_to_sql(flat):
#     conn = pyodbc.connect('Driver={SQL Server};'
#                       'Server=localhost;'
#                       'Database=fb_snp;'
#                       'Trusted_Connection=yes;')
#     cursor = conn.cursor()
#     for row in flat:
#         # print(row)
#         sql = "insert into dbo.post_clicks_by_type (id, period, name, title, description, LinkClicks, OtherClicks, PhotoView, VideoPlay) values (?,?,?,?,?,?,?,?,?) "
#         insert_news=(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7],row[8])
#         cursor.execute(sql, insert_news)
#         # cursor.executemany("insert into fb_snp.fb_data[id], [period], [name], [value], [end_time], [title], [description]) values (%('id')s, %('period')s, %('name')s, %('value')s, %('end_time')s,%('title')s,%('description')s",tuple(row))
#         conn.commit()


def get_post_insights(post_id):
    return graph.get_connections(
            id=post_id,
            connection_name="insights",
            metric = 'post_clicks_by_type',
            period="lifetime",
            show_description_from_api_doc=False,
        )

if '__name__==__main__':
#set constraint in db để tránh bị trùng
 # proxies = {
    #     "http": "172.16.0.53:8080",
    #     "https": "172.16.0.53:8080"
    # }

    # graph = fb.GraphAPI(access_token=page_token, version="3.1",  proxies=proxies)
    graph = fb.GraphAPI(access_token=page_token, version="3.1") 
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost;'
                      'Database=fb_snp;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    # vì lấy 1 lần rất lâu nên chỉnh ra thời gian từng khúc để nhanh hơn và tránh lỗi (#190) của facebook
    cursor.execute("select post.id from post left join post_clicks_by_type on post.id = post_clicks_by_type.id where post_clicks_by_type.post_id is null and post.date between '2021-01-01' and '2021-03-31' order by date desc")
    lstPostIds = cursor.fetchall()

    for postId in lstPostIds:
        post_insight = get_post_insights(postId[0])
        dfs = post_insight['data']
        flat = flatten_json(dfs,postId[0])

        cursor1 = conn.cursor()
        sql = "insert into dbo.post_clicks_by_type (id, period, name, title, description, LinkClicks, OtherClicks, PhotoView, VideoPlay,post_id) values (?,?,?,?,?,?,?,?,?,?)"
        cursor1.fast_executemany = True
        params = list(tuple(record) for record in flat)
        # print(params)
        cursor1.executemany(sql, params)
        
        cursor1.commit()
        cursor1.close()

    cursor.close()
    conn.close()
# những bài post mà có story (cập nhật ảnh đại diện hay ảnh bìa) thì ko có dữ liệu nên khi đụng đến nó sẽ đưa ra dữ liệu empty và tự ngừng
# là những post có id có story và created_time nên loại trừ những post này 
# có những post có id khác nhau nhưng lại có nội dung giống nhau 