import json
import facebook as fb
from datetime import datetime
import pandas as pd
from pandas.io.json import json_normalize
#import csv
#from IPython.display import display
#from sqlalchemy import create_engine
import pyodbc

page_id='217328504988428'
page_token = 'EAAyBEgkHZCbYBANms9s1kPc3Fwrw3fr9OfZCRDIlJT1hQTfhzlidpUt3irjLqd4EjI4F1KYlEbBkHGm1obIJ1iZC7Hf8da9aU7ZAJsOGCPFlDhUKTM32yr6tJmsPmdhFurmipGis6YxHdQYLdEUZBzuITg1Ynzl6C4w3PzxhJfQZDZD'
graph = fb.GraphAPI(access_token=page_token, version="3.1")

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
            end_time = end_time.split('T')
            end_time = end_time[0]
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
        sql = "insert into dbo.page_insight (id, period, name, value, end_time, title, description) values (?,?,?,?,?,?,?) "
        
        # if isinstance(row[3], dict):
        #     value = ', '.join(row[3].keys())
        # else:
        #     value = row[3]
        insert_news=(row[0], row[1], row[2], row[3], row[4], row[5], row[6])
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
    sql=f"delete from dbo.page_insight where end_time between '{from_date}' and '{to_date}'"
    # print(sql)
    find = cursor.execute(sql)
    conn.commit()
    return find


if '__name__==__main__':
    # from_date = datetime(2020, 8, 31)
    # to_date = datetime(2020, 11, 1)
    from_date = datetime(2018, 12, 31)
    to_date = datetime(2019, 4, 1)

    # print(from_date)
    page_insights = graph.get_connections(
                        id = page_id,
                        connection_name = 'insights',
                        metric = '''
                                    page_views_total,page_post_engagements,
                                    page_fans,page_fan_adds_unique,page_fan_removes_unique,
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
                                    page_total_actions,page_get_directions_clicks_logged_in_unique,
                                    page_call_phone_clicks_logged_in_unique,page_website_clicks_logged_in_unique
                                ''',
                        period = 'day',
                        since = from_date,
                        until = to_date,
                        show_description_from_api_doc = False
                )

    # page_cta_clicks_logged_in_unique
    dfs = page_insights['data']
    '''đoạn này chạy nhiều sợ facebook tưởng hack nên cho dô khuôn và thay bằng mở file json'''
    # f = open("e:/github/-Getting-Facebook-Data/code/fb.json",)
    # data=json.load(f)
    # dfs=data["data"]

    delete(from_date, to_date)
    flat=flatten_json(dfs)
    save_to_sql(flat)
    print('OK')

