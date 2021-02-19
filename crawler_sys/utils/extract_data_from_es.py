# -*- coding: utf-8 -*-
"""
Created on Mon Oct  8 17:14:16 2018

@author: fangyucheng
"""

import elasticsearch


hosts = '192.168.17.11'
port = 80
user_id = 'fangyucheng'
password = 'VK0FkWf1fV8f'
http_auth = (user_id, password)
lose_re_url = []
es = elasticsearch.Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)

search_body = {"query": {
                         "bool": {
                                  "filter": [
                                             {"term": {"platform.keyword": "new_tudou"}},
                                             {"term": {"post_by.keyword": "zhangqiongzi"}}
                                            ]
                                  }
                        }
                }


get_tr = es.search(index='target_releasers', body=search_body, size=200)

result_lst = []

for line in get_tr['hits']['hits']:
    result_lst.append(line['_source'])