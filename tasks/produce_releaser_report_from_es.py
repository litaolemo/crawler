# -*- coding:utf-8 -*-
# @Time : 2019/4/24 17:51 
# @Author : litao
# 提供账号和平台,生成数据报告
# 数据维度：发布量、播放量、粉丝量、评论量、点赞量，视频url、时长、发布时间、发布账号

# import time
import json
# import argparse
import datetime
from elasticsearch import Elasticsearch
import pandas as pd
from elasticsearch.helpers import scan
from func_find_week_num import find_week_belongs_to
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler
from crawler.crawler_sys.utils import trans_format
from func_cal_doc_id import cal_doc_id

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

data_dic = {}
miaopai_list = []
file = r'D:\work_file\无锡台内容数据需求.csv'
with open(file, 'r')as f:
    header_Lst = f.readline().strip().split(',')
    for line in f:
        line_Lst = line.strip().split(',')
        line_dict = dict(zip(header_Lst, line_Lst))
        releaser = line_dict['releaser']
        platform = line_dict['platform']
        # releaserUrl = func_search_reUrl_from_target_index(platform, releaser)
        releaserUrl = 1
        if releaserUrl != None:
            re_list = []
            search_body = {
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"platform.keyword": platform}}, {"term": {"releaser.keyword": releaser}},
                            {"range": {"release_time": {"gte": 1546272000000, "lt": 1554048000000}}},
                            {"range": {"fetch_time": {"gte": 1556150400000}}}
                        ]
                    }
                }
            }

            scan_re = scan(client=es, index='crawler-data-raw', doc_type='doc',
                           query=search_body, scroll='3m')
            for one_scan in scan_re:
                "发布者,平台,标题,url,播放量,点赞量,评论量,时长,发布时间"
                data_dic[cal_doc_id(platform, url=one_scan["_source"]["url"], doc_id_type='all-time-url')]=[one_scan["_source"]["releaser"],one_scan["_source"]["platform"],one_scan["_source"]["title"],one_scan["_source"]["url"],one_scan["_source"]["play_count"],one_scan["_source"]["favorite_count"],one_scan["_source"]["comment_count"],one_scan["_source"]["duration"],datetime.datetime.fromtimestamp(one_scan["_source"]["release_time"]/1000).strftime('%Y-%m-%d %H:%M:%S')]
data_lis = []
print(len(data_dic))
for d in data_dic:
    data_lis.append(data_dic[d])

data = pd.DataFrame(data_lis)
data.to_csv('./%s.csv' % "无锡台内容数据需求2", encoding="ansi")


