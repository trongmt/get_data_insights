import json
import facebook as fb
from datetime import datetime
import pandas as pd
from pandas.io.json import json_normalize
import csv
from IPython.display import display
from sqlalchemy import create_engine
import psycopg2
import pyodbc

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
    json_dataframe=pd.DataFrame(flat,columns= ["id", "period", "name", "value", "end_time", "title", "description"])
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-3P6F3VK;'
                      'Database=fb_snp;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    for i,row in json_dataframe.iterrows():
        sql = "insert into dbo.fb_data (id, period, name, value, end_time, title, description) values (?,?,?,?,?,?,?) "
        cursor.execute(sql, tuple(row))
        # cursor.excutemany(f"insert into fb_snp.fb_data (id, period, name, value, end_time, title, description) values (" + "%s,"*(len(row)-1) + "%s) ")
        conn.commit()


def delete(from_date, to_date):
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-3P6F3VK;'
                      'Database=fb_snp;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()                 
    # drop existing records
    sql="delete from dbo.fb_data where end_time between '{from_date}' and '{to_date}'"
    conn.commit()
    find = cursor.execute(sql)
    return find


if '__name__==__main__':
    # from_date = datetime(2020, 8, 31)
    # to_date = datetime(2020, 11, 1)
    from_date = datetime(2020, 12, 29)
    to_date = datetime(2021, 1, 1)

    '''đoạn này chạy nhiều sợ facebook tưởng hack nên cho dô khuôn và thay bằng mở file json'''
    f = open("e:/github/-Getting-Facebook-Data/code/fb.json",)
    data=json.load(f)
    dfs=data["data"]

    delete(from_date, to_date)
    flat=flatten_json(dfs)
    save_to_sql(flat)

