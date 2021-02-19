# -*- coding: utf-8 -*-
"""
Created on Fri Nov 23 13:48:33 2018

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

unsolve_lst = []
result_lst2 =[]

for line in task_list:
    url = line['url']
    title = line['title']
    search_body = {"query": {"bool": {"filter": [{"term": {"title.keyword": title}}]}}}
    search = es_connection.search(index="test2", doc_type="fyc1210", body=search_body)
    if search["hits"]["total"] == 0:
        unsolve_lst.append(url)
        print("can not get video data at %s" % url)
    else:
        video_data = search["hits"]["hits"][0]["_source"]
        result_lst2.append(video_data)
        print("get playcount at %s" % url)

lst_to_csv(listname=result_lst2,
           csvname="F:/add_target_releaser/Nov/Sep2.csv")