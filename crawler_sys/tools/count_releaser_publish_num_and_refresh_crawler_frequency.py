# -*- coding: utf-8 -*-
"""
Created on Tue Dec 11 11:59:55 2018

@author: fangyucheng
"""

import json
import time
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

bulk_all_body = ''
count = 0
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
    task_list.remove(line)
    total = releaser_info['Nov_2018']
    if total >= 900:
        releaser_info['frequency'] = 9
        print("%s frequency is 3" % releaser_info['releaser'])
    if total >= 300:
        releaser_info['frequency'] = 3
        print("%s frequency is 3" % releaser_info['releaser'])
        count += 1
    else:
        releaser_info['frequency'] = 1
    _id = platform + '_' + releaser
    bulk_head = '{"index": {"_id":"%s"}}' % _id
    releaser_info['timestamp'] = int(time.time() * 1e3)
    data_str = json.dumps(releaser_info, ensure_ascii=False)
    bulk_one_body = bulk_head+'\n'+data_str+'\n'
    bulk_all_body += bulk_one_body
    es.bulk(index='target_releasers', doc_type='doc',
            body=bulk_all_body)
    bulk_all_body = ''
    print('write %s into es' % releaser)
