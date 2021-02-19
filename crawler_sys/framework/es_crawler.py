# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 17:20:57 2018

@author: hanye
"""

import time
from elasticsearch.helpers import scan
from crawler_sys.framework.es_ccr_index_defination import es_framework
from crawler_sys.framework.es_ccr_index_defination import index_crawler_raw
from crawler_sys.framework.es_ccr_index_defination import doc_type_crawler_raw
from crawler_sys.framework.es_ccr_index_defination import index_url_register
from crawler_sys.framework.es_ccr_index_defination import doc_type_url_register
from crawler_sys.framework.func_calculate_newTudou_video_id import calculate_newTudou_video_id
from crawler_sys.framework.func_calculate_toutiao_video_id import calculate_toutiao_video_id
from crawler_sys.framework.func_calculate_wangyi_news_id import calculate_wangyi_news_id

def scan_crawler_raw_index(search_body):
    total_hit, scan_resp = scan_index(index=index_crawler_raw,
                                      doc_type=doc_type_crawler_raw,
                                      search_body=search_body)
#    search_resp = es_framework.search(index=index_crawler_raw,
#                                      doc_type=doc_type_crawler_raw,
#                                      body=search_body,
#                                      size=0, request_timeout=100)
#    total_hit = search_resp['hits']['total']
#    print('Index: %s total hit: %d'
#          % (index_crawler_raw, total_hit))
#    if total_hit>0:
#        scan_resp = scan(client=es_framework,
#                         query=search_body,
#                         index=index_crawler_raw,
#                         doc_type=doc_type_crawler_raw,
#                         request_timeout=300)
#    else:
#        print('Zero hit.')
#        scan_resp = None

    return (total_hit, scan_resp)

def scan_crawler_url_register(search_body):
    total_hit, scan_resp = scan_index(index=index_url_register,
                                      doc_type=doc_type_url_register,
                                      search_body=search_body)
    return (total_hit, scan_resp)


def scan_index(index, doc_type, search_body):
    search_resp = es_framework.search(index=index,
                                      doc_type=doc_type,
                                      body=search_body,
                                      size=0,
                                      request_timeout=100)
    total_hit = search_resp['hits']['total']
    print('Index: %s total hit: %d'
          % (index, total_hit))
    if total_hit > 0:
        scan_resp = scan(client=es_framework,
                         query=search_body,
                         index=index,
                         doc_type=doc_type,
                         request_timeout=300)
    else:
        print('Zero hit.')
        scan_resp = None

    return (total_hit, scan_resp)

def construct_id_for_url_register(platform, url):
    if platform == 'new_tudou':
        vid_bare = calculate_newTudou_video_id(url)
        vid = 'new_tudou_%s' % vid_bare
    elif platform == 'toutiao':
        vid_bare = calculate_toutiao_video_id(url)
        vid = 'toutiao_%s' % vid_bare
    elif platform == '腾讯新闻':
        c_time = str(int(time.time()))
        vid = "tencent_news_%s_%s" % (url, c_time)
    elif platform == '网易新闻':
        vid = "163_news_%s" % calculate_wangyi_news_id(url)
    else:
        vid_bare = url
        vid = vid_bare
    return vid
