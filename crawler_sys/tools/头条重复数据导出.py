# -*- coding: utf-8 -*-
"""
Created on Thu Feb 14 16:12:57 2019

@author: zhouyujiang

查找切片中头条发布者+发布时间+duration相同的数据
"""

import pandas as pd
import datetime
import elasticsearch
from elasticsearch.helpers import scan
from crawler_url_video_info import get_target_video_info
hosts='192.168.17.11'
port=80
user='zhouyujiang'
passwd='8tM9JDN2LVxM'
http_auth=(user, passwd)
es=elasticsearch.Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)



zyj_set = set()
zyj_dict = {}
index = 'short-video-weekly'
doc_type = 'daily-url-2019_w07_s1'
re_s_t = 1549728000000
re_e_t = 1550332800000
count = 0 
sacn_body =  {  
            "query": {
                "bool": {
                  "filter": [
                    {"term": {"platform.keyword": 'toutiao'}},
                    {"range": {"release_time": {"gte": re_s_t,"lt":re_e_t}}}
                    ]
                }
                  }
                   }
scan_re = scan(client=es, index=index, doc_type=doc_type,
              query=sacn_body, scroll='3m')
for one in scan_re:
    count = count +1
    if count %1000 == 0:
        print(count)
    line = one['_source']
    releaser = line['releaser']
    release_time = line['release_time']
    duration = line['duration']
    zyj_id = releaser + str(release_time) + str(duration)
    if zyj_id not in zyj_dict:
        zyj_dict[zyj_id] = []
        zyj_dict[zyj_id].append(line)
    else:
        zyj_set.add(zyj_id)
        zyj_dict[zyj_id].append(line)
re_list = []
for one_key in zyj_set:
    for one_value in zyj_dict[one_key]:
#        url = one_value['url']
#        new_playcount = get_target_video_info(url=url, platform='toutiao')
#        one_value['new_playcount'] = new_playcount
        re_list.append(one_value)
        

    
            

data = pd.DataFrame(re_list)
data.to_csv('头条7zhou重复数据重新抓取播放量.csv')
        
        