# -*- coding: utf-8 -*-
"""
Created on Fri Nov 23 14:44:07 2018

@author: fangyucheng
"""


from elasticsearch import Elasticsearch
from crawler.crawler_sys.utils.trans_format import lst_to_csv


hosts = '192.168.17.11'
port = 80
user_id = 'fangyucheng'
password = 'VK0FkWf1fV8f'
http_auth = (user_id, password)
es_connection = Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)

unsolve2_lst = []
result_lst2 =[]

for url in unsolve_lst:
    search_body = {"query": {"bool": {"filter": [{"term": {"url.keyword": url}}]}}}
    search = es_connection.search(index="test2", doc_type="fyc1123", body=search_body)
    if search["hits"]["total"] == 0:
        unsolve2_lst.append(url)
        print("can not get video data at %s" % url)
    else:
        video_data = search["hits"]["hits"][0]["_source"]
        result_lst2.append(video_data)
        print("get playcount at %s" % url)

lst_to_csv(listname=result_lst2,
           csvname="F:/add_target_releaser/Nov/get_playcount_by_releaser2.csv")