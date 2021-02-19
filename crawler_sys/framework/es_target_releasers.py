# -*- coding: utf-8 -*-
"""
Created on Wed Jun  6 18:13:14 2018

@author: hanye
"""
import json #, redis
import random
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

#rds=redis.StrictRedis(host='192.168.17.26',port=6379,db=0)

es_framework = Elasticsearch(hosts='192.168.17.11', port=80,
                                http_auth=('crawler', 'XBcasfo8dgfs'))

index_target_releaser = 'target_releasers'
doc_type_target_releaser = 'doc'

def bulk_write_target_releasers(dict_Lst,
                                index=index_target_releaser,
                                doc_type=doc_type_target_releaser):
    bulk_write_body=''
    write_counter=0
    for line in dict_Lst:
        write_counter+=1
        try:
            releaser=line['releaser']
            platform=line['platform']
            doc_id_releaser='%s_%s' % (platform, releaser)
            action_str=('{ "index" : { "_index" : "%s", "_type" : "%s","_id" : "%s" } }'
                        % (index_target_releaser, doc_type_target_releaser, doc_id_releaser) )
            data_str=json.dumps(line, ensure_ascii=False)
            line_body = action_str + '\n' + data_str + '\n'
            bulk_write_body += line_body
        except:
            print('ill-formed data', line)
        if write_counter%1000==0 or write_counter==len(dict_Lst):
            print('Writing into es %d/%d' % (write_counter, len(dict_Lst)))
            if bulk_write_body!='':
                es_framework.bulk(body=bulk_write_body, request_timeout=100)

def get_releaserUrls_from_es(platform,
                             releaser=None,
                             frequency=None,
                             target_index=None,
                             project_tags=[]):
    search_body = {"query": {"bool": {"filter": [{"term": {"platform.keyword": platform}}]}}}
    if releaser is not None:
        releaser_dict = {"term": {"releaser.keyword": releaser}}
        search_body['query']['bool']['filter'].append(releaser_dict)
    if frequency is not None:
        frequency_dict = {"range": {"frequency": {"gte": frequency}}}
        search_body['query']['bool']['filter'].append(frequency_dict)
    if project_tags:
        frequency_dict = {"terms":{"project_tags.keyword":project_tags}}
        search_body['query']['bool']['filter'].append(frequency_dict)
    # print(target_index,doc_type_target_releaser,search_body)
    search_resp=es_framework.search(index=target_index,
                                     doc_type=doc_type_target_releaser,
                                     body=search_body,
                                     size=0,
                                     request_timeout=100)
    total_hit = search_resp['hits']['total']
    releaserUrl_Lst = []
    if total_hit > 0:
        print('Got %d releaserUrls for platform %s.' % (total_hit, platform))
        scan_resp = scan(client=es_framework, query=search_body,
                         index=target_index,
                         doc_type=doc_type_target_releaser,
                         request_timeout=200)
        for line in scan_resp:
            try:
                releaserUrl = line['_source']['releaserUrl']
                releaser = line['_source']['releaser']

                releaserUrl_Lst.append((releaserUrl,releaser))
            except:
                print('error in :' ,line)
                continue
    else:
        print('Got zero hits.')
    return releaserUrl_Lst
