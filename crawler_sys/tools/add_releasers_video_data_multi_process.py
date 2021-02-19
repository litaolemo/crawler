# -*- coding: utf-8 -*-
"""
Created on Thu Sep  6 09:22:24 2018

@author: fangyucheng
"""

import time
from multiprocessing import Pool
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler
from crawler.crawler_sys.utils import trans_format
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

hosts = '192.168.17.11'
port = 80
user = 'zhouyujiang'
passwd = '8tM9JDN2LVxM'
http_auth = (user, passwd)

es = Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)

def func_search_reUrl_from_target_index(platform, releaser):
    search_body = {
            "query": {
                "bool": {
                  "filter": [
                    {"term": {"platform.keyword": platform}},
                    {"term": {"releaser.keyword": releaser}}
                    ]
                }
                  }
                   }
    search_re = es.search(index='target_releasers', doc_type='doc', body=search_body)
    if search_re['hits']['total'] > 0:
        return search_re['hits']['hits'][0]['_source']['releaserUrl']
    else:
        print('Can not found:', platform, releaser)
        return None

def get_target_releaser_video_info(file_name,
                                   output_to_es_raw=True,
                                   es_index=None,
                                   doc_type=None,
                                   releaser_page_num_max=10000):
    start_time = int(time.time()*1e3)
    task_lst = trans_format.csv_to_lst_with_headline(file_name)
    pool = Pool(10)
    arg_dict = {"releaser_page_num_max": releaser_page_num_max,
                "output_to_es_raw": True,
                "es_index": es_index,
                "doc_type": doc_type}
    for line in task_lst:
        platform = line['platform']
        releaser = line['releaser']
        try:
            releaserUrl = line["releaserUrl"]
        except:
            releaserUrl = func_search_reUrl_from_target_index(platform, releaser)
        print("releaserUrl",releaserUrl)
        crawler_initialization = get_crawler(platform=platform)
        try:
            crawler = crawler_initialization().search_page
            pool.apply_async(crawler, args=(releaserUrl, ), kwds=arg_dict)
        except:
            continue
    pool.close()
    pool.join()
    end_time = int(time.time()*1e3)
    time_info = [start_time, end_time]
    return time_info

if __name__ =='__main__':
    get_time = get_target_releaser_video_info(file_name=r'C:\Users\litao\Desktop\target_releasers - key_custom.csv',
                                              releaser_page_num_max=300,
                                              es_index='crawler-data-raw',
                                              doc_type='doc')