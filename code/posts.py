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
        devices = self.ParserPost(df)

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
        devices = self.ParserPostInsight(df)

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
        devices = self.ParserPostClicks(df)

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
     
    def FlattenPostActivity(self, df,orgPostid):
        flattened_data =[]
        devices = self.ParserPostActivity(df)

        for device in devices:
            id, period, name, title, description, share, like, comment= device
            flattened_data.append([id, period, name, title, description, share, like, comment, orgPostid])
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

            sql = f"delete from dbo.{table_name} where PostDate between '{from_date}' and '{to_date}'"
            cur.execute(sql)
            
            conn.commit()
            cur.close()
        except (Exception, pyodbc.DatabaseError) as error:
            return f'ERR: {error}'
        finally:
            if conn is not None:
                self.ConnectToClose(conn)
    
    def Post(self, graph, from_date, to_date):
        self.PrepareData('Post', from_date, to_date)
        all_post = graph.get_all_connections(id=page_id,connection_name='posts',since=from_date,until=to_date)
        for ind, post in enumerate(all_post):        
            # post = self.PostConnection(graph, page_id, 'posts', from_date, to_date)
            # self.PrepareData('Post', from_date, to_date)

            flat = self.FlattenPost(post)
            for row in flat:
                # print(row)
                sql = "insert into dbo.Post (PostDate, Message, ID) values (?,?,?) "
                header=(row[0], row[1], row[2])

                self.SaveToDB(sql, header)

    def PostInsight(self,graph):
        # get post id from db
        conn = None
        conn = self.ConnectionSqlDb(conn_str_config)
        cur = conn.cursor()
        cur.execute("select Post.ID from Post left join PostInsight on Post.ID = PostInsight.ID where PostInsight.ID is null order by PostDate desc")
        lstPostIds = cur.fetchall()

        for postId in lstPostIds:
    
            metric= '''
                post_engaged_users,
                post_engaged_fan,post_clicks,post_clicks_unique,
                post_impressions,post_impressions_unique,post_impressions_paid,
                post_impressions_paid_unique,post_impressions_fan,post_impressions_fan_unique,
                post_activity'''
            dfs = self. GraphConnection(graph, postId[0], 'insights', metric, 'lifetime')
            flat = self.FlattenPostInsight(dfs)

            for row in flat:
                # print(row)
                sql = "insert into dbo.PostInsight (ID, Period, Name, Value, Title, Description) values (?,?,?,?,?,?) "
                header=(row[0], row[1], row[2], row[3], row[4], row[5])
                self.SaveToDB(sql, header)

    def PostClick(self, graph):
        conn = None

        conn = self.ConnectionSqlDb(conn_str_config)
        cur = conn.cursor()
        cur.execute("select Post.ID from Post left join PostClick on Post.ID = PostClick.ID where PostClick.ID is null order by PostDate desc")
        lstPostIds = cur.fetchall()

        for postId in lstPostIds:
            dfs = self. GraphConnection(graph, postId[0], 'insights', 'post_clicks_by_type', 'lifetime')
            flat = self.FlattenPostClicks(dfs,postId[0])

            cursor1 = conn.cursor()
            sql = "insert into dbo.PostClick (ID, Period, Name, Title, Description, LinkClicks, OtherClicks, PhotoView, VideoPlay, PostID) values (?,?,?,?,?,?,?,?,?,?)"
            cursor1.fast_executemany = True
            params = list(tuple(record) for record in flat)
            cursor1.executemany(sql, params)
            
            cursor1.commit()
            cursor1.close()
    
    def PostActivity(self, graph):
        conn = None

        conn = self.ConnectionSqlDb(conn_str_config)
        cur = conn.cursor()
        cur.execute("select Post.ID from Post left join PostActivity on Post.ID = PostActivity.ID where PostActivity.ID is null order by PostDate desc")
        lstPostIds = cur.fetchall()

        for postId in lstPostIds:
            dfs = self. GraphConnection(graph, postId[0], 'insights', 'post_activity_by_action_type', 'lifetime')
            flat = self.FlattenPostActivity(dfs,postId[0])

            cursor1 = conn.cursor()
            sql = "insert into dbo.PostActivity (ID, Period, Name, Title, Description, Share, [Like], Comment, PostID) values (?,?,?,?,?,?,?,?,?)"
            cursor1.fast_executemany = True
            params = list(tuple(record) for record in flat)
            cursor1.executemany(sql, params)
            
            cursor1.commit()
            cursor1.close()
    
    def GraphConnection(self, graph, post_id, connection_name, metric, period):
        post_insights = graph.get_connections(
                            id = post_id,
                            connection_name = connection_name,
                            metric = metric,
                            period = period,
                            show_description_from_api_doc = False)

        dfs = post_insights['data']
        return dfs

if __name__=='__main__':
    # proxies = {
    #     "http": "172.16.0.53:8080",
    #     "https": "172.16.0.53:8080"
    # }
    # graph = fb.GraphAPI(access_token=page_token, version="3.1",  proxies=proxies)
    graph = fb.GraphAPI(access_token = page_token, version="3.1")
    from_date = datetime(2020, 12, 31)
    to_date = datetime(2021, 4, 1)
    ps = Posts()

    ps.Post(graph, from_date, to_date)
    ps.PostInsight(graph)
    ps.PostClick(graph)
    ps.PostActivity(graph)