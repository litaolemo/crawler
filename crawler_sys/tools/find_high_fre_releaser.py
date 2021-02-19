# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 10:03:53 2018

@author: fangyucheng
"""

import configparser
from elasticsearch import Elasticsearch

hosts = '192.168.17.11'
port = 80
user_id = 'fangyucheng'
password = 'VK0FkWf1fV8f'
http_auth = (user_id, password)
es_connection = Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)

search_body = {"query":{"bool":{"filter":[{"term":{"platform.keyword":"haokan"}}]}},
               "sort":[{"Nov_2018":{"order":"desc"}}]}

es_search = es_connection.search(index='target_releasers',
                                 doc_type='doc',
                                 body=search_body, size=1000)

es_data_lst = es_search['hits']['hits']

result_list = []

for line in es_data_lst:
    data_dic = line['_source']
    result_list.append(data_dic)

new_list = result_list[:40]

result_list = []

releaser_dic = {}
for line in new_list:
    releaser_dic[line['releaser']] = line['releaserUrl']


config = configparser.ConfigParser()
config['haokan'] = releaser_dic

with open ('high_fre.ini', 'w', encoding='utf-8') as ini:
    config.write(ini)



#special task
#for line in source_lst:
#    detail_lst = line['detail']
#    csm_mdu = detail_lst[0]['csm_mdu']
#    for detail_dic in detail_lst:
#        detail_dic.pop('csm_mdu')
#    line['csm_mdu'] = csm_mdu