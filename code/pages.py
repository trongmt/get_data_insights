#!C:\ProgramData\Miniconda3
# -*- coding: utf-8 -*-

import json
import facebook as fb
from datetime import datetime
import pandas as pd
from pandas.io.json import json_normalize
import pyodbc

page_id='217328504988428'
page_token = 'EAAyBEgkHZCbYBANms9s1kPc3Fwrw3fr9OfZCRDIlJT1hQTfhzlidpUt3irjLqd4EjI4F1KYlEbBkHGm1obIJ1iZC7Hf8da9aU7ZAJsOGCPFlDhUKTM32yr6tJmsPmdhFurmipGis6YxHdQYLdEUZBzuITg1Ynzl6C4w3PzxhJfQZDZD'
conn_str_config = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=snp_fanpage;UID=sa;PWD=admin123$'

class Pages:
    def ConnectionSqlDb(self, connStr):
        conn = None
        try:
            conn = pyodbc.connect(connStr, autocommit=True)
            return conn

        except (Exception, pyodbc.DatabaseError) as error:
            return f'ERR: {error}'

    def ConnectToClose(self, conn):
        conn.close()

    def ParserPageInsights(self, data):
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

    def FlattenInsights(self, df):
        flattened_data =[]
        devices = self.ParserPageInsights(df)

        for device in devices:
            id, period, name, value, end_time, title, description = device
            flattened_data.append([id, period, name, value, end_time, title, description])
        return flattened_data

    def ParserPageFanGender(self, data):
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
                
                F1 = value["F.13-17"]
                F2 = value["F.18-24"]
                F3 = value["F.25-34"]
                F4 = value["F.35-44"]
                F5 = value["F.45-54"]
                F6 = value["F.55-64"]
                F7 = value["F.65+"]
                M1 = value["M.13-17"]
                M2 = value["M.18-24"]
                M3 = value["M.25-34"]
                M4 = value["M.35-44"]
                M5 = value["M.45-54"]
                M6 = value["M.55-64"]
                M7 = value["M.65+"]
                devices.append([id, period, name, end_time, title, description,F1,F2,F3,F4,F5,F6,F7,M1,M2,M3,M4,M5,M6,M7])
        return devices

    def FlattenFanGender(self, df):
        flattened_data =[]
        devices = self.ParserPageFanGender(df)
        
        for device in devices:
            id, period, name, end_time, title, description,F1,F2,F3,F4,F5,F6,F7,M1,M2,M3,M4,M5,M6,M7 = device
            flattened_data.append([id, period, name, end_time, title, description,F1,F2,F3,F4,F5,F6,F7,M1,M2,M3,M4,M5,M6,M7])
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

            sql = f"delete from dbo.{table_name} where EndTime between '{from_date}' and '{to_date}'"
            cur.execute(sql)
            
            conn.commit()
            cur.close()
        except (Exception, pyodbc.DatabaseError) as error:
            return f'ERR: {error}'
        finally:
            if conn is not None:
                self.ConnectToClose(conn)

    def PageInsights(self, graph, from_date, to_date):        
        metric = '''
                    page_fans,
                    page_impressions_unique,
                    page_impressions_paid_unique,
                    page_impressions_organic_unique,
                    page_engaged_users,
                    page_consumptions,
                    page_post_engagements,
                    page_video_views,
                    page_fan_adds_unique,
                    page_fan_removes_unique'''
        dfs = self.GraphConnection(graph, page_id, 'insights', metric, 'day', from_date, to_date)
        
        # dfs = page_insights['data']
        self.PrepareData('PageInsight', from_date, to_date)

        flat = self.FlattenInsights(dfs)
        for row in flat:
            sql = '''insert into dbo.PageInsight 
                           (ID, Period, Name, Value, EndTime, Title, Description) 
                    values (?,?,?,?,?,?,?)
                '''
            header = (row[0], row[1], row[2], row[3], row[4], row[5], row[6])

            self.SaveToDB(sql, header)        

    def PageFansGenderAge(self, graph, from_date, to_date):
        dfs = self.GraphConnection(graph, page_id, 'insights', 'page_fans_gender_age', 'day', from_date, to_date)

        # dfs = page_insights['data']
        self.PrepareData('PageFans', from_date, to_date)

        flat = self.FlattenFanGender(dfs)
        flat = pd.DataFrame(flat,columns=['ID','Period','Name','EndTime','Title','Description','F1','F2','F3','F4','F5','F6','F7','M1','M2','M3','M4','M5','M6','M7'])
        flat=pd.melt(flat,id_vars = ['ID','Period','Name','EndTime','Title','Description'],value_vars=['F1','F2','F3','F4','F5','F6','F7','M1','M2','M3','M4','M5','M6','M7'],var_name='Attribute', value_name='Value')
        
        for index,row in flat.iterrows():
            sql = '''insert into dbo.PageFans 
                            (ID, Period, Name, EndTime, Title, Description, Attribute, Value) 
                    values (?,?,?,?,?,?,?,?)
                '''
            header = (row.ID,row.Period,row.Name,row.EndTime,row.Title,row.Description,row.Attribute,row.Value)
            
            self.SaveToDB(sql, header)

    def GraphConnection(self, graph, page_id, connection_name, metric, period, from_date, to_date):
        page_insights = graph.get_connections(
                            id = page_id,
                            connection_name = connection_name,
                            metric = metric,
                            period = period,
                            since = from_date,
                            until = to_date,
                            show_description_from_api_doc = False)

        dfs = page_insights['data']
        return dfs

if __name__=='__main__':
    # proxies = {
    #     "http": "172.16.0.53:8080",
    #     "https": "172.16.0.53:8080"
    # }
    # graph = fb.GraphAPI(access_token=page_token, version="3.1",  proxies=proxies)
    graph = fb.GraphAPI(access_token = page_token, version="3.1")
    from_date = datetime(2020, 12, 30)
    to_date = datetime(2021, 3, 1)
    pg = Pages()

    pg.PageInsights(graph, from_date, to_date)
    pg.PageFansGenderAge(graph, from_date, to_date)
