# -*- coding: utf-8 -*-
"""
Created on Fri Nov 23 14:44:07 2018

@author: fangyucheng
"""

import elasticsearch.helpers
from elasticsearch import Elasticsearch
from crawler.crawler_sys.utils.trans_format import lst_to_csv
from crawler.crawler_sys.utils.trans_format import str_file_to_lst
from crawler.crawler_sys.utils.trans_format import str_lst_to_file
#from crawler.crawler_sys.utils.trans_format import csv_to_lst_with_headline

hosts = '192.168.17.11'
port = 80
user_id = 'fangyucheng'
password = 'VK0FkWf1fV8f'
http_auth = (user_id, password)
es_connection = Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)


def init_task_list(file_path=None):
    task_list = []
    if file_path is None:
        es_scan = elasticsearch.helpers.scan(es_connection, index='album-play-count')
        for line in es_scan:
            video_dict = line['_source']
            task_list.append(video_dict)
        return task_list
    else:
        task_list = str_file_to_lst(file_path)
        return task_list

unsolve_lst = []
result_lst2 =[]


task_list = str_file_to_lst('F:/add_target_releaser/album_play_count/dec')
#task_list = init_task_list()


for line in task_list:
    try:
        if type(line) == dict:
            url = line['url']
        elif type(line) == str:
            url = line
        search_body = {"query": {"bool": {"filter": [{"term": {"url.keyword": url}}]}}}
        search = es_connection.search(index="test2", doc_type="dec", body=search_body)
        if search["hits"]["total"] == 0:
            unsolve_lst.append(url)
            print("can not get video data at %s" % url)
        else:
            video_data = search["hits"]["hits"][0]["_source"]
            result_lst2.append(video_data)
            print("get playcount at %s" % url)
    except:
        pass

lst_to_csv(listname=result_lst2,
           csvname="F:/add_target_releaser/last_month/fix_play_count12242.csv")
str_lst_to_file(unsolve_lst,
                filename="F:/add_target_releaser/last_month/unsolved")
