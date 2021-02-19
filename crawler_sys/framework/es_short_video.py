# -*- coding: utf-8 -*-
"""
Created on Wed Jun  6 18:14:32 2018

@author: hanye
"""

import datetime
import json
from elasticsearch import Elasticsearch
# from crawler_sys.framework.short_video_vid_cal_func import vid_cal_func
from write_data_into_es.func_cal_doc_id import cal_doc_id

es_framework = Elasticsearch(hosts='192.168.17.11', port=80,
                             http_auth=('crawler', 'XBcasfo8dgfs'))

index_target_releaser = 'target_releasers'
doc_type_target_releaser = 'doc'

index_short_video = 'short-video-production'
index_short_video_all_time_url = 'short-video-all-time-url'
doc_type_short_video_DU = 'daily-url'
doc_type_short_video_ATU = 'all-time-url'


def cal_vid(platform, url):
    pass


def bulk_write_short_video(dict_Lst,
                           index=index_short_video,
                           index_all_time_url=index_short_video_all_time_url,
                           doc_type_daily=doc_type_short_video_DU,
                           doc_type_ATU=doc_type_short_video_ATU,
                           write_daily=True,
                           write_ATU=True):
    """
    If not explictly specified, will write daily-url and all-time-url.
    """
    if write_daily is False and write_ATU is False:
        return None
    else:
        bulk_write_SV_bd_daily = ''
        bulk_write_SV_bd_ATU = ''
        write_counter = 0
        for line in dict_Lst:
            try:
                url = line['url']
                platform = line['platform']
                fetch_time_ts = line['fetch_time']
                fetch_time_T = datetime.datetime.fromtimestamp(fetch_time_ts / 1e3)
                fetch_time_day_str = fetch_time_T.isoformat()[:10]
                id_daily = cal_doc_id(platform, url=url, doc_id_type='daily-url',data_dict=line,fetch_day_str=fetch_time_day_str)
                id_ATU = cal_doc_id(platform, url=url, doc_id_type='all-time-url',data_dict=line)

                data_str = json.dumps(line, ensure_ascii=False)
                if write_daily is True:
                    action_str_daily = '{"index": {"_id":"%s"}}' % id_daily
                    line_body_for_daily = action_str_daily + '\n' + data_str + '\n'
                    bulk_write_SV_bd_daily += line_body_for_daily
                if write_ATU is True:
                    action_str_ATU = '{"index": {"_id":"%s"}}' % id_ATU
                    line_body_for_ATU = action_str_ATU + '\n' + data_str + '\n'
                    bulk_write_SV_bd_ATU += line_body_for_ATU
                write_counter += 1
            except:
                pass
        t1 = datetime.datetime.now()
        if write_daily is True and bulk_write_SV_bd_daily != '':
            bulk_resp = es_framework.bulk(index=index, doc_type=doc_type_daily,
                                          body=bulk_write_SV_bd_daily,
                                          request_timeout=200)
            if bulk_resp['errors'] is True:
                print(bulk_resp)
            t2 = datetime.datetime.now()
            td = t2 - t1
            #            print(bulk_resp)
            print('written %d lines into %s, costs %s,'
                  % (write_counter, doc_type_daily, td),
                  datetime.datetime.now())
            bulk_write_SV_bd_daily = ''
        t3 = datetime.datetime.now()
        if write_ATU is True and bulk_write_SV_bd_ATU != '':
            es_framework.bulk(index=index_all_time_url, doc_type=doc_type_ATU,
                              body=bulk_write_SV_bd_ATU, request_timeout=200)
            t4 = datetime.datetime.now()
            tdd = t4 - t3
            print('written %d lines into %s, costs %s,'
                  % (write_counter, doc_type_ATU, tdd),
                  datetime.datetime.now())
            bulk_write_SV_bd_ATU = ''
