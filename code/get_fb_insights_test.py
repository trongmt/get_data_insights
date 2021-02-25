import json
import facebook as fb
from datetime import datetime
import pandas as pd
import csv
from pandas.io.json import json_normalize
import os

# page_id='217328504988428'
# page_token = 'EAAyBEgkHZCbYBANms9s1kPc3Fwrw3fr9OfZCRDIlJT1hQTfhzlidpUt3irjLqd4EjI4F1KYlEbBkHGm1obIJ1iZC7Hf8da9aU7ZAJsOGCPFlDhUKTM32yr6tJmsPmdhFurmipGis6YxHdQYLdEUZBzuITg1Ynzl6C4w3PzxhJfQZDZD'
# graph = fb.GraphAPI(access_token=page_token, version="3.1")

def checkList(ele, prefix):
    for i in range(len(ele)):
        if (isinstance(ele[i], list)):
            checkList(ele[i], prefix+"["+str(i)+"]")
        #elif (isinstance(ele[i], str)):
            #printField(ele[i], prefix+"["+str(i)+"]")
        else:
            checkDict(ele[i], prefix+"["+str(i)+"]")

def checkDict(jsonObject, prefix):
    for ele in jsonObject:
        if (isinstance(jsonObject[ele], dict)):
            checkDict(jsonObject[ele], prefix+"."+ele)

        elif (isinstance(jsonObject[ele], list)):
            checkList(jsonObject[ele], prefix+"."+ele)

        #elif (isinstance(jsonObject[ele], str)):
            #printField(jsonObject[ele],  prefix+"."+ele)

def printField(df1, ele, prefix):
    #print (prefix, ":" , ele)
    df = pd.DataFrame(df1)
    #df = DataFrame (People_List, columns=['First_Name','Last_Name','Age'])
    df.to_csv('insights3.csv', index = False)

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
            devices.append([id, period, name, value, end_time, title, description])
            
    return devices

def flatten_json(df, output_name):
    flattened_data =[]
    
    #display(df)
    devices = get_values(df)

    for device in devices:
        id, period, name, value, end_time, title, description = device
        flattened_data.append([id, period, name, value, end_time, title, description])
    
    json_to_csv(flattened_data, output_name)

    return "FLATTENED DATA SAVED"

def flatten_json2(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[str(name[:-1])] = str(x)

    flatten(y)
    return out

def json_to_csv(normalized_df, csv_name):

    header = ["id", "period", "name", "value", "end_time", "title", "description"]

    with open(csv_name, 'wt', newline ='') as file:
        writer = csv.writer(file, delimiter=',')
        writer.writerow(i for i in header)
        for j in normalized_df:
            writer.writerow(j)
        
    return "FLATTENED DATA SAVED"

# if '__name__==__main__':
#     page_insights = graph.get_connections(
#                         id = page_id,
#                         connection_name = 'insights',
#                         metric = '''page_total_actions,page_views_total,page_preview_total,page_fan_adds_unique,
#                                     page_impressions_unique,page_post_engagements,page_video_views,
#                                     page_daily_follows_unique''',
#                         period = 'day',
#                         since = datetime(2019, 1, 1),
#                         until = datetime(2019, 1, 2),
#                         show_description_from_api_doc = False
#                 )
    
#     #df = pd.DataFrame(page_insights['data'])
    
#     dfs = page_insights['data']
# đóng lại phần lấy dữ liệu từ facebook, dùng file json
f = open("e:/github/get_data_insights/code/fb.json",)
data=json.load(f)
dfs=data["data"]
    #display(dfs)
    
    #for df in dfs:
    #    #print(df)
    #    for element in df:
    #        #If Json Field value is a Nested Json
    #        if (isinstance(df[element], dict)):
    #            checkDict(df[element], element)
    #            
    #        #If Json Field value is a list
    #        elif (isinstance(df[element], list)):
    #            checkList(df[element], element)
    #            
    #        #If Json Field value is a string
    #        elif (isinstance(df[element], str)):
    #            printField(df, df[element], element)
    
flatten_json(dfs,"flattened_file.csv")