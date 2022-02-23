import eia
import pandas as pd
import json
api_key = "huB9HhJVZ737qVBnAxt9Ce1nIbLqtx1I1JPltZk4"
api = eia.API(api_key)

# ### Retrieve Data By Keyword ###
# keyword_search = api.data_by_keyword(keyword=['crude oil', 'price'],
#                                      filters_to_keep=['brent'],
#                                      filters_to_remove=['AEO2015'],
#                                      rows=1000)
#
# for key,value in keyword_search.items():
#     print(key,value)
#
# df = pd.DataFrame(keyword_search)

# ### Retrieve Data by Category ID ###
# category_search = api.data_by_keyword(category=3390110,
#                                       filters_to_keep=['brent'],
#                                       filters_to_remove=['AEO2015'])
#
# for key,value in category_search.items():
#     print(key,value)
#
# df = pd.DataFrame(category_search)


# ### Retrieve Data By Date Last Updated ###
# date_search = api.data_by_date(date='2015-01-01T00:00:00Z
#                                           TO 2015-01-01T23:59:59Z',
#                                filters_to_keep=['brent'],
#                                filters_to_remove=['AEO2015'],
#                                rows=1000)
#
# for key,value in date_search.items():
#     print(key,value)
#
# df = pd.DataFrame(date_search)


### Retrieve Data By Series ID ###

series_id = 'EBA.MIDA-ALL.NG.COL.HL'
series_search = api.data_by_series(series=series_id)
# print(series_search)
# for key, value in series_search.items():
#     print(key, value)

df = pd.DataFrame(series_search)
print(df.head())

# import urllib.request, urllib.error
# import requests
#
# url = f"https://api.eia.gov/series/?api_key={api_key}&series_id={series_id}&out=json"
# url = f"http://api.eia.gov/series/?series_id={api_key}&api_key={series_id}&out=json"
#
# print(requests.get(url).json()['series'][0]['data'])
#
#
# stockdata = urllib.request.urlopen(url)
# data = stockdata.read().decode()
#
# js = json.loads(data)
# print(js)
