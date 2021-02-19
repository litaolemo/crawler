# -*- coding: utf-8 -*-
"""
Created on Tue Dec 11 11:59:55 2018

@author: fangyucheng
"""

import elasticsearch
import elasticsearch.helpers

hosts = '192.168.17.11'
port = 80
user_id = 'fangyucheng'
password = 'VK0FkWf1fV8f'
http_auth = (user_id, password)

es = elasticsearch.Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)

task_list = []
result_list = []

es_scan = elasticsearch.helpers.scan(es, index='target_releasers')

for line in es_scan:
    task_list.append(line)
print('the length of releaser is %s' % len(task_list))

for line in task_list:
    releaser_info = line['_source']
    platform = releaser_info['platform']
    releaser = releaser_info['releaser']
    search_body = {"query":{"bool":{"filter":[{"term":{"platform.keyword":platform}},
                                              {"term":{"releaser.keyword":releaser}},
                                              {"term":{"data_month":11}},
                                              {"term":{"data_year":2018}},
                                              {"term":{"stats_type.keyword":"new_released"}}]}}}

    es_search = es.search(index='releaser', doc_type='releasers',
                          body=search_body)
    if es_search['hits']['total'] != 0:
        hits = es_search['hits']['hits'][0]['_source']['video_num']
        releaser_info['Nov_2018'] = int(hits)
        print("releaser %s hit %s video in es" % (releaser, hits))
    else:
        releaser_info['Nov_2018'] = 0
    result_list.append(releaser_info)
    task_list.remove(line)
    
count = 0
for line in result_list:
    total = line['Nov_2018']
    if total >= 900:
        line['frequency'] = 9
        print("%s frequency is 3" % line['releaser'])
    if total >= 300:
        line['frequency'] = 3
        print("%s frequency is 3" % line['releaser'])
        count += 1
    else:
        line['frequency'] = 1

print("the number of flood releaser is %s" % count)