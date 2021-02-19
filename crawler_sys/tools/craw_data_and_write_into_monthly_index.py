# -*- coding: utf-8 -*-
"""
Created on Thu Sep  6 09:22:24 2018

@author: fangyucheng
"""

#import time
import json 
#import argparse
import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler
from func_cal_doc_id import cal_doc_id
try:
    from crawler_sys.framework.func_get_releaser_id import *
except:
    from func_get_releaser_id import *

hosts = '192.168.17.11'
port = 80
user = 'zhouyujiang'
passwd = '8tM9JDN2LVxM'
http_auth = (user, passwd)

es = Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)

#parser = argparse.ArgumentParser()
#parser.add_argument('-w', '--week_str', type=str, default=None)


    
#def find_week_belongs_to(dayT, week_start_iso_weekday=1):
#    if isinstance(dayT, datetime.datetime):
#        dayD = datetime.date(dayT.year, dayT.month, dayT.day)
#    elif isinstance(dayT, datetime.date):
#        dayD = dayT
#    else:
#        print('Wrong type, input parameter must be instance of datetime.datetime '
#              'or datetime.date!')
#        return None
#    calendar_tupe = dayD.isocalendar()
#    param_iso_week_no = calendar_tupe[1]
#    param_iso_weekday = calendar_tupe[2]
#    param_iso_week_year = calendar_tupe[0]
#    if isinstance(week_start_iso_weekday, int) and week_start_iso_weekday>0:
#        if week_start_iso_weekday==1: # iso week, Mon is weekday 1, Sun is weekday 7
#            cal_week_no = param_iso_week_no
#            cal_week_year = param_iso_week_year
#            cal_weekday = param_iso_weekday
#            return (cal_week_year, cal_week_no, cal_weekday)
#    else:
#        print('Wrong parameter, must be positive int!')
#        return None

def get_target_releaser_video_info(platform,
                                   releaserUrl,
                                   log_file=None,

                                   output_to_es_raw=True,
                                   es_index=None,
                                   doc_type=None,
                                   releaser_page_num_max=100):
        if log_file == None:
            log_file = open('error.log' ,'w')

        crawler = get_crawler(platform=platform)
        crawler_initialization = crawler()
        if platform == 'haokan':
            try:
                crawler_initialization.releaser_page(releaserUrl=releaserUrl,
                                                     releaser_page_num_max=releaser_page_num_max,
                                                     output_to_es_raw=True,
                                                     es_index=es_index,
                                                     doc_type=doc_type,
                                                     fetchFavoriteCommnt=True)
            except:
                print(releaserUrl, platform, file=log_file)
        else:
            try:
                crawler_initialization.releaser_page(releaserUrl=releaserUrl,
                                                     releaser_page_num_max=releaser_page_num_max,
                                                     output_to_es_raw=True,
                                                     es_index=es_index,
                                                     doc_type=doc_type)
            except:
                print(releaserUrl, platform, file=log_file)



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


def func_write_into_monthly_index_new_released(line_list, doc_type, index='short-video-production-2020'):
    count = 0 
    bulk_all_body = ''
    re_list = []
    for line in line_list:
        count = count + 1
        # try:
        #     monthly_net_inc_favorite_count_net_inc_play_count = line['play_count']
        # except:
        #     monthly_net_inc_favorite_count_net_inc_play_count = line['play_count']
        monthly_net_inc_favorite_count_net_inc_play_count = line['play_count']
        monthly_net_inc_favorite_count_net_inc_comment_count = line['comment_count']
        monthly_net_inc_favorite_count_net_inc_favorite_count = line['favorite_count']
        try:
            monthly_net_inc_repost_count_net_inc_favorite_count = line['repost_count']
        except:
            monthly_net_inc_repost_count_net_inc_favorite_count = 0
        monthly_net_inc_favorite_count_cal_base = 'accumulate'
        timestamp = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)

        line.update({
                'timestamp': timestamp,
                'monthly_cal_base': monthly_net_inc_favorite_count_cal_base,
                'monthly_net_inc_favorite_count': monthly_net_inc_favorite_count_net_inc_favorite_count,
                'monthly_net_inc_comment_count': monthly_net_inc_favorite_count_net_inc_comment_count,
                'monthly_net_inc_play_count': monthly_net_inc_favorite_count_net_inc_play_count,
                'monthly_net_inc_repost_count': monthly_net_inc_repost_count_net_inc_favorite_count,
        })
        re_list.append(line)
        # if 'video_id' in line.keys():
        #     line.pop('video_id')
        url = line['url']
        platform = line['platform']
        doc_id = cal_doc_id(platform, url=url, doc_id_type='all-time-url',data_dict=line)
        bulk_head = '{"index": {"_id":"%s"}}' % doc_id
        data_str = json.dumps(line, ensure_ascii=False)

        bulk_one_body = bulk_head + '\n' + data_str + '\n'
        #
        bulk_all_body += bulk_one_body
        if count%500 == 0:

             eror_dic=es.bulk(index=index, doc_type=doc_type,
                    body=bulk_all_body, request_timeout=200)
             bulk_all_body=''
             if eror_dic['errors'] is True:
                 print(eror_dic['items'])
                 print(bulk_all_body)
             print(count)

    if bulk_all_body != '':
     eror_dic = es.bulk(body=bulk_all_body,
                        index=index,
                        doc_type=doc_type ,
                        request_timeout=200)
     if eror_dic['errors'] is True:
         print(eror_dic)
    print(count)
            
#def func_search_data_to_write_from_index_craw(re_s, re_e,releaser,platform):
#    search_body = {  
#            "query": {
#                "bool": {
#                  "filter": [
#                    {"term": {"platform.keyword": platform}},
#                    {"term": {"releaser.keyword": releaser}},
#                    {"range": {"release_time": {"gte": re_s,"lt":re_e}}}
#                    ]
#                }
#                  }
#                   }

re_s_t = 1582992000000
re_e_t = 1585670400000
miaopai_list = []
monthly_doc_type_name = 'daily-url-2020-03-31'
count_has = 0
# file = r'D:\work_file\发布者账号\一次性需求附件\brief-7月整月数据（含融合）.csv'
file = r'D:\work_file\发布者账号\一次性需求附件\month3month2_releaser_video_num预警 2020-04-03.csv'
# file = r'D:\work_file\word_file_new\PROJECT_ORDINARY_COURSE\03_releaser_analysis\for_production_org\target_releasers - key_custom - 副本.csv'
# file = r'D:\work_file\发布者账号\一次性需求附件\month10month9_releaser_video_num预警 2019-11-04.csv'
file = r'D:\wxfile\WeChat Files\litaolemo\FileStorage\File\2020-04\少了数据的快手两个号.csv'
with open(file, 'r',encoding="gb18030")as f:
    header_Lst = f.readline().strip().split(',')
    for line in f:
        line_Lst = line.strip().split(',')
        line_dict = dict(zip(header_Lst, line_Lst))
        releaser = line_dict['releaser']
        platform = line_dict['platform']
        #_uid = line_dict['id']
        try:
            releaserUrl = line_dict['releaserUrl']
        except:
            releaserUrl = func_search_reUrl_from_target_index(platform, releaser)
        try:
            releaser_id_str= platform + "_" + get_releaser_id(platform=platform,releaserUrl=releaserUrl)
        except:
            print("error_relaser_id",releaser,platform,releaserUrl)
            continue
        print(releaser,platform)
        if platform == 'miaopai' or platform == "抖音":
            miaopai_list.append(releaser_id_str)


        if releaserUrl != None:
            re_list = []
            # get_target_releaser_video_info(
            #                               platform=platform,
            #                               releaserUrl=releaserUrl,
            #                               releaser_page_num_max=200,
            #                               es_index='crawler-data-raw',
            #                               doc_type='doc'
            #                               )
            if platform in [
                   #"toutiao",
                  #  "haokan",
                   # "new_tudou",
                   # "腾讯视频",
                   # "网易新闻",
                   #  "miaopai",
                   # "腾讯新闻",
                   "kwai",
                   # "抖音"
            ]:
                pass
            else:
                continue
            search_body = {
                    "query": {
                            "bool": {
                                    "filter": [
                                           # {"term": {"platform.keyword": platform}},
                                            # {"term": {"platform.keyword": "toutiao"}},
                                            #{"term": {"data_provider.keyword": "CCR"}},
                                            {"term": {"releaser_id_str": releaser_id_str}},
                                            # {"term": {"releaser.keyword": releaser}},
                                            {"range": {"release_time": {"gte": re_s_t, "lt": re_e_t}}},
                                            # {"range": {"fetch_time": {"gte": 1570670089000 }}}
                                           #{"range": {"fetch_time": {"gte": re_e_t, "lt": 1585843200000}}},
                                           # {"range": {"comment_count": {"gte": 1}}}
                                    ]
                            }
                    }
            }
            scan_re = scan(client=es, index='short-video-all-time-url', doc_type='all-time-url',
                           query=search_body, scroll='3m')
            # scan_re = scan(client=es, index='crawler-data-raw', doc_type="doc",
            #               query=search_body, scroll='3m')
            # scan_re = scan(client=es, index='short-video-production', doc_type="daily-url",
            #                query=search_body, scroll='3m')
            for one_scan in scan_re:
                re_list.append(one_scan['_source'])
                count_has += 1
                if count_has % 500 == 0:
                    print(count_has)
                    print(len(re_list))
                    func_write_into_monthly_index_new_released(re_list, doc_type=monthly_doc_type_name)
                    re_list = []
            func_write_into_monthly_index_new_released(re_list, doc_type=monthly_doc_type_name)
            # break
# for releaser in miaopai_list:
#    re_list = []
#    search_body = {
#                "query": {
#                    "bool": {
#                      "filter": [
#                        #{"term": {"platform.keyword": 'miaopai'}},
#                        {"term": {"releaser_id_str": releaser}},
#                        {"range": {"release_time": {"gte": re_s_t,"lt":re_e_t}}},
#                      # {"range": {"fetch_time": {"gte": 1550505600000}}}
#                        ]
#                    }
#                      }
#                      }
#    scan_re = scan(client=es, index='short-video-all-time-url', doc_type='all-time-url',
#                    query=search_body, scroll='3m')
#    for one_scan in scan_re:
#        re_list.append(one_scan['_source'])
#    func_write_into_monthly_index_new_released(re_list, doc_type=monthly_doc_type_name)
