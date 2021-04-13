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
            # dict
            # print(value)
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
            # devices.append([id, period, name, end_time, title, description,F1,F2,F3,F4,F5,F6,F7,M1,M2,M3,M4,M5,M6,M7,U1,U2,U3,U4])
            # F.13-17,F.18-24,F.25-34,F.35-44,F.45-54,F.55-64,F.65+,M.13-17,M.18-24,M.25-34,M.35-44,M.45-54,M.55-64,M.65+,U.18-24,U.25-34,U.35-44
    return devices

def flatten_json(df):
    flattened_data =[]
    #display(df)
    devices = get_values(df)
    for device in devices:
        id, period, name, end_time, title, description,F1,F2,F3,F4,F5,F6,F7,M1,M2,M3,M4,M5,M6,M7 = device
        flattened_data.append([id, period, name, end_time, title, description,F1,F2,F3,F4,F5,F6,F7,M1,M2,M3,M4,M5,M6,M7])
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
    for index,row in flat.iterrows():
        # print(row)
        sql = "insert into dbo.Pagefans (ID, Period, Name, EndTime, Title, Description, Attribute, Value) values (?,?,?,?,?,?,?,?) "
        # insert_news=(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
        insert_news=(row.ID,row.Period,row.Name,row.EndTime,row.Title,row.Description,row.Attribute,row.Value)
        # print(row[7])
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
    sql=f"delete from dbo.Pagefans where EndTime between '{from_date}' and '{to_date}'"
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
    from_date = datetime(2020, 1, 1)
    to_date = datetime(2020, 2, 1)

    # print(from_date)
    page_insights = graph.get_connections(
                        id = page_id,
                        connection_name = 'insights',
                        metric = '''page_fans_gender_age''',
                        period = 'day',
                        since = from_date,
                        until = to_date,
                        show_description_from_api_doc = False)

    # page_cta_clicks_logged_in_unique
    dfs = page_insights['data']
    # print(dfs)
    '''đoạn này chạy nhiều sợ facebook tưởng hack nên cho dô khuôn và thay bằng mở file json'''
    # f = open("e:/github/-Getting-Facebook-Data/code/fb.json",)
    # data=json.load(f)
    # dfs=data["data"]

    delete(from_date, to_date)
    flat=flatten_json(dfs)
    flat = pd.DataFrame(flat,columns=['ID','Period','Name','EndTime','Title','Description','F1','F2','F3','F4','F5','F6','F7','M1','M2','M3','M4','M5','M6','M7'])
    flat=pd.melt(flat,id_vars = ['ID','Period','Name','EndTime','Title','Description'],value_vars=['F1','F2','F3','F4','F5','F6','F7','M1','M2','M3','M4','M5','M6','M7'],var_name='Attribute', value_name='Value')
    # print(flat)
    save_to_sql(flat)
    print('OK')

