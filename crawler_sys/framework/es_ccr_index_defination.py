# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 17:09:09 2018

@author: hanye
"""
from elasticsearch import Elasticsearch

es_framework = Elasticsearch(hosts='192.168.17.11', port=80,
                             http_auth=('crawler', 'XBcasfo8dgfs'))

index_target_releaser = 'target_releasers'
doc_type_target_releaser = 'doc'

index_short_video = 'short-video-production'
doc_type_short_video_DU = 'daily-url'
doc_type_short_video_ATU = 'all-time-url'

index_crawler_raw = 'crawler-data-raw'
doc_type_crawler_raw = 'doc'

index_url_register = 'crawler-url-register'
doc_type_url_register = 'doc'
fields_url_register = ['platform', 'url', 'video_id',
                       'release_time', 'timestamp']