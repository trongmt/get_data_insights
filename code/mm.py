import json
import facebook as fb
from datetime import datetime
import pandas as pd
from pandas.io.json import json_normalize

page_id='217328504988428'
page_token = 'EAAyBEgkHZCbYBANms9s1kPc3Fwrw3fr9OfZCRDIlJT1hQTfhzlidpUt3irjLqd4EjI4F1KYlEbBkHGm1obIJ1iZC7Hf8da9aU7ZAJsOGCPFlDhUKTM32yr6tJmsPmdhFurmipGis6YxHdQYLdEUZBzuITg1Ynzl6C4w3PzxhJfQZDZD'
graph = fb.GraphAPI(access_token=page_token, version="3.1")

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
    # df = json_normalize(df1)
    # df = pd.DataFrame (df1, columns=[df['name'],df['period'],df['tiltle'],df['description'],df['values']])
    # df.to_csv('insights3.csv', index = False)
    # df = json.loads(df1)
    # values_data=df['values']['value']
       
if '__name__==__main__':
    page_insights = graph.get_connections(
                        id = page_id,
                        connection_name = 'insights',
                        metric = '''page_total_actions,page_views_total,page_preview_total,page_fan_adds_unique,
                                    page_impressions_unique,page_post_engagements,page_video_views,
                                    page_daily_follows_unique''',
                        period = 'day',
                        since = datetime(2019, 1, 1),
                        until = datetime(2019, 1, 2),
                        show_description_from_api_doc = False
                )
    
    #df = pd.DataFrame(page_insights['data'])
    dfs = page_insights['data']
    #print(df)
    result=[]
    for df in dfs:
        # print(df)
        for element in df:
            #If Json Field value is a Nested Json
            if (isinstance(df[element], dict)):
                checkDict(df[element], element)
                
            #If Json Field value is a list
            elif (isinstance(df[element], list)):
                checkList(df[element], element)
                
            #If Json Field value is a string
            elif (isinstance(df[element], str)):
                printField(df, df[element], element)
        result.append(df)
    # print(result)
    # out_json=json.dumps(result)
    # print(out_json)
    # print(type(result)) type là list
    a = json_normalize(result)
    print(a)
#sao nó chỉ chạy ra có mỗi dòng cuối cùng thôi vạy

#type là dict
# data_string = json.dumps(df, indent=4)
# print(type(json_string))
#type la str
# data = json.loads(df) 
# --báo lỗi phải là kiểu string vậy lệnh này kko đưa json thành string, mà dùng cho kiểu string để biến thành dict
# print(type(data))
# for name in df['values']:
#      print(name['value'])
    # data=pd.DataFrame(df)
    # print(data)
#record_list = json.loads(df)   
#multiple_level_data = pd.json_normalize(
                            #page_insights, 
                            #record_path = ['data'], 
                            #meta =['name', 'period', 'title', 'description'], 
                            #meta_prefix='config_params_'
                            #record_prefix='dbscan_'
                        #)
#df = df.drop({'values'}, axis=1)

# Saving to CSV format
# df.to_csv('insights3.csv', index = False)
#print('OK')