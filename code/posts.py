import json
import facebook as fb
from datetime import datetime
import pandas as pd
from pandas.io.json import json_normalize
import pyodbc

page_id='217328504988428'
page_token = 'EAAyBEgkHZCbYBANms9s1kPc3Fwrw3fr9OfZCRDIlJT1hQTfhzlidpUt3irjLqd4EjI4F1KYlEbBkHGm1obIJ1iZC7Hf8da9aU7ZAJsOGCPFlDhUKTM32yr6tJmsPmdhFurmipGis6YxHdQYLdEUZBzuITg1Ynzl6C4w3PzxhJfQZDZD'
conn_str_config = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=snp_fanpage;UID=sa;PWD=admin123$'

class Posts:
    def ConnectionSqlDb(self, connStr):
        conn = None
        try:
            conn = pyodbc.connect(connStr, autocommit=True)
            return conn

        except (Exception, pyodbc.DatabaseError) as error:
            return f'ERR: {error}'

    def ConnectToClose(self, conn):
        conn.close()

    def ParserPost(self, data):
        devices = []

        if len(data) > 2:
            if 'message' in data:
                created_time = data["created_time"]
                created_time = created_time.split('T')
                date = created_time[0]
                message = data['message']
                id = data["id"]
                devices.append([date,message,id])

        if len(data) ==2:
            if 'message' not in data:
                created_time = data["created_time"]
                created_time = created_time.split('T')
                date = created_time[0]
                id = data["id"]
                message = ''
                devices.append([date,message,id])

        return devices

    def FlattenPost(self, df):
        flattened_data =[]
        devices = get_values(df)

        for device in devices:
            date,message,id = device
            flattened_data.append([date,message,id])       
        return flattened_data

    def ParserPostInsight(self, data):
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
                devices.append([id, period, name, value, title, description])
            
        return devices
    
    def FlattenPostInsight(self, df):
        flattened_data =[]
        devices = get_values(df)

        for device in devices:
            id, period, name, value, title, description = device
            flattened_data.append([id, period, name, value, title, description])
        return flattened_data

    def ParserPostClicks(self, data):
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
                if len(value) >= 0:
                    VideoPlay = value['video play'] if 'video play' in value else 0
                    OtherClicks = value['other clicks'] if 'other clicks' in value else 0
                    PhotoView = value['photo view'] if 'photo view' in value else 0
                    LinkClicks = value['link clicks'] if 'link clicks' in value else 0

                    devices.append([id, period, name, title, description, LinkClicks, OtherClicks, PhotoView, VideoPlay])
                
        return devices
    
    def FlattenPostClicks(self, df, orgPostid):
        flattened_data =[]
        devices = get_values(df)

        for device in devices:
            id, period, name, title, description, LinkClicks, OtherClicks, PhotoView, VideoPlay = device
            flattened_data.append([id, period, name, title, description, LinkClicks, OtherClicks, PhotoView, VideoPlay, orgPostid])
        return flattened_data   

    def ParserPostActivity(self, data):
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
                if len(value) >= 0:
                    share = value['share'] if 'share' in value else 0
                    like = value['like'] if 'like' in value else 0
                    comment = value['comment'] if 'comment' in value else 0
                    devices.append([id, period, name, title, description, share, like, comment])
                
        return devices
     
    def FlattenPostActivity(self, df):
        flattened_data =[]
        devices = get_values(df)

        for device in devices:
            id, period, name, title, description, share, like, comment= device
            flattened_data.append([id, period, name, title, description, share, like, comment])
        return flattened_data

    def SaveToDB(self, sql, headers):
        conn = None
        try:
            # create a cursor object for execution
            conn = self.ConnectionSqlDb(conn_str_config)
            cur = conn.cursor()

            cur.execute(sql, headers)
            
            conn.commit()
            cur.close()

        except (Exception, pyodbc.DatabaseError) as error:
            return f'ERR: {error}'
        finally:
            if conn is not None:
                self.ConnectToClose(conn)

    def PrepareData(self, table_name, from_date, to_date):
        conn = None
        try:
            # create a cursor object for execution
            conn = self.ConnectionSqlDb(conn_str_config)
            cur = conn.cursor()

            sql = f"delete from dbo.{table_name} where end_time between '{from_date}' and '{to_date}'"
            cur.execute(sql)
            
            conn.commit()
            cur.close()
        except (Exception, pyodbc.DatabaseError) as error:
            return f'ERR: {error}'
        finally:
            if conn is not None:
                self.ConnectToClose(conn)
    
    def Post(self, graph):
        post = self.


    def PostInsight():


    def PostClicks():

    
    def PostActivity():

    
    
    def GraphConection(self, graph, post_id, connection_name, metric, period):
        post_insights = graph.get_connections(
                            id = post_id,
                            connection_name = connection_name,
                            metric = metric,
                            period = period,
                            show_description_from_api_doc = False)

        dfs = post_insights['data']
        return dfs       