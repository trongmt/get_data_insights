# get_data_insights
- Thư viện urllib3 nên có phiên bản là 1.26.2.

# Troubleshoot:
- requests.exceptions.ConnectionError: 
	HTTPSConnectionPool(host='graph.facebook.com', port=443): Max retries exceeded with url: /v10.0/217328504988428/insights?access_token=EAAyBEgkHZCbYBANms9s1kPc3Fwrw3fr9OfZCRDIlJT1hQTfhzlidpUt3irjLqd4EjI4F1KYlEbBkHGm1obIJ1iZC7Hf8da9aU7ZAJsOGCPFlDhUKTM32yr6tJmsPmdhFurmipGis6YxHdQYLdEUZBzuITg1Ynzl6C4w3PzxhJfQZDZD&period=day&metric=page_views_total,page_post_engagements,page_fans,page_fan_adds_unique (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x00000161D5E08E80>: Failed to establish a new connection: [WinError 10060] A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond'))

Giải pháp: Liên hệ ATTT về policy

- 503: service_unavailable, unavailable
Lỗi xảy ra khi thiếu khai báo proxies nên bị chặn bởi policy công ty.

Giải pháp:
Thiếu proxies, headers

	proxies = {
        "http": "172.16.0.53:8080"
    }

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Safari/537.36'}

    js = requests.get(url, proxies=proxies, headers=headers, verify=False)

Cách lấy proxies, headers: Paste link vào browser => F12 (Inspect) => Network (menu) => Header => Ctrl+R (Perform a request) to record the reload.

- 500: internal_server_error, server_error, /o\, ✗


https://requests.readthedocs.io/en/master/api/
https://www.w3schools.com/python/ref_requests_get.asp
https://developers.facebook.com/docs/marketing-api/insights/

https://drgabrielharris.medium.com/python-how-making-facebook-api-calls-using-facebook-sdk-ea18bec973c8

# link facebook graph api liên quan
https://developers.facebook.com/docs/graph-api/reference/v3.1/post
https://developers.facebook.com/docs/graph-api/reference/v10.0/insights

#ver1 - 19/03/2021:
-các bản code chính: page_insight, post, page_fans_gender_age,post_activity_action_by_type,page_consumptions_by_type
-power bi: SNP-fanpage-v1
