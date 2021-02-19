# -*- coding: utf-8 -*-
"""
Created on Thu Sep  6 09:22:24 2018

@author: fangyucheng
"""

# import time
import json
# import argparse
import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from write_data_into_es.func_get_releaser_id import *
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler

from func_cal_doc_id import cal_doc_id

hosts = '192.168.17.11'
port = 80
user = 'zhouyujiang'
passwd = '8tM9JDN2LVxM'
http_auth = (user, passwd)

es = Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)


# parser = argparse.ArgumentParser()
# parser.add_argument('-w', '--week_str', type=str, default=None)


# def find_week_belongs_to(dayT, week_start_iso_weekday=1):
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
        log_file = open('error.log', 'w')

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
        #print('Can not found:', platform, releaser)
        return None


def func_write_into_monthly_index_new_released(line_list, doc_type, index='short-video-production-2020'):
    count = 0
    bulk_all_body = ''
    re_list = []
    for line in line_list:
        count = count + 1
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
        # print(line)
        # print(url)
        doc_id = cal_doc_id(line['platform'], url=url, doc_id_type='all-time-url',data_dict=line)
        bulk_head = '{"index": {"_id":"%s"}}' % doc_id
        data_str = json.dumps(line, ensure_ascii=False)

        bulk_one_body = bulk_head + '\n' + data_str + '\n'
        #
        bulk_all_body += bulk_one_body
        if count % 500 == 0:

            eror_dic = es.bulk(index=index, doc_type=doc_type,
                               body=bulk_all_body, request_timeout=200)
            bulk_all_body = ''
            if eror_dic['errors'] is True:
                print(eror_dic['items'])
                print(bulk_all_body)
            #print(count)

    if bulk_all_body != '':
        eror_dic = es.bulk(body=bulk_all_body,
                           index=index,
                           doc_type=doc_type,
                           request_timeout=200)
        if eror_dic['errors'] is True:
            print(eror_dic)
    #print(count)

# def func_search_data_to_write_from_index_craw(re_s, re_e,releaser,platform):
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
file = r'D:\work_file\5月补数据 - 副本.csv'
file = r'D:\work_file\发布者账号\一次性需求附件\1-8月all_all-time-url.csv'
file = r'D:\work_file\发布者账号\month1month0_releaser_video_num预警 2020-02-03.csv'
file = r'D:\work_file\发布者账号\一次性需求附件\month3month2_releaser_video_num预警 2020-04-03.csv'
# file = r'D:\work_file\发布者账号\月数据账号\brief-JYH月度账号数据需求.csv'
# file = r'D:\work_file\5月补数据.csv'
# file = r'D:\work_file\word_file_new\PROJECT_ORDINARY_COURSE\03_releaser_analysis\for_production_org\target_releasers - key_custom - 副本.csv'
# file = r'Z:\CCR\数据输出\罗佳\0214-关键词需求\账号列表 的副本.csv'
#file = r'D:\work_file\发布者账号\一次性需求附件\珠三角微信、短视频、APP账号.csv'
# file = r'D:\work_file\5月补数据 - 副本.csv'
# file = r'D:\work_file\发布者账号\一次性需求附件\附件2：生活这一刻新迁移.csv'
now = int(datetime.datetime.now().timestamp()*1e3)
with open(file, 'r',encoding="gb18030")as f:
    header_Lst = f.readline().strip().split(',')
    for line in f:
        line_Lst = line.strip().split(',')
        line_dict = dict(zip(header_Lst, line_Lst))
        releaser = line_dict['releaser']
        platform = line_dict['platform']
        releaserUrl = line_dict['releaserUrl']
        #doc_id = line_dict['releaser_id_str']
        line_dict["releaser_id"] = get_releaser_id(platform=platform, releaserUrl=releaserUrl)
        if line_dict["releaser_id"]:
            doc_id = line_dict['platform'] + '_' + line_dict['releaser_id']
        else:
            print("error_url ",releaserUrl)
            continue
        #line_dict["releaser_id"] = line_dict['releaser_id_str']
        # print(releaser, platform,doc_id)
        if platform == 'miaopai' or platform == "抖音":
            miaopai_list.append(doc_id)
        # if not releaserUrl:
        #     releaserUrl = func_search_reUrl_from_target_index(platform, releaser)
        if True:
            re_list = []
            # get_time = get_target_releaser_video_info(
            #                               platform=platform,
            #                               releaserUrl=releaserUrl,
            #                               releaser_page_num_max=100,
            #                               es_index='crawler-data-raw',
            #                               doc_type='doc'
            #                               )
            if platform in [
                   # "toutiao",
                   #  "haokan",
                   #  "new_tudou",
                   # "腾讯视频",
                   # "网易新闻",
                    "miaopai",
                   # "腾讯新闻",
                   # "kwai",
                   # "抖音"
            ]:
                pass
            else:
                continue
            search_body = {
                "query": {
                    "bool": {

                        "filter": [
                            {"term": {"platform.keyword": platform}},
                            {"term": {"releaser_id_str": doc_id}},
                          #{"term": {"releaser.keyword": releaser}},
                            {"range": {"release_time": {"gte": re_s_t, "lt": re_e_t}}},
                          # {"range": {"fetch_time": {"gte": 1579017600000,"lt":1580659200000}}}
                        # {"range": {"fetch_time": {"lte": 1581523200000}}}
                        ]
                    }
                }
            }
            # if line_dict["releaser_id"]:
            #     search_body["query"]["bool"]["filter"].pop(1)
            #     search_body["query"]["bool"]["filter"].append({"term": {"releaser_id_str": doc_id}})
            # scan_re = scan(client=es, index='crawler-data-raw', doc_type='doc',
            #                query=search_body, scroll='3m')
            scan_re = scan(client=es, index='short-video-all-time-url', doc_type='all-time-url',query=search_body, scroll='3m')
            # scan_re = scan(client=es, index='short-video-production', doc_type='daily-url',
            #                query=search_body, scroll='3m')
            count_has = 0
            for one_scan in scan_re:

                doc_id = cal_doc_id(one_scan["_source"]["platform"], url=one_scan["_source"]["url"],
                                    doc_id_type='all-time-url',
                                    data_dict=one_scan["_source"])
                find_exist = {
                    "query": {
                        "bool": {
                            "filter": [
                                {"term": {"_id": doc_id}}
                            ]
                        }
                    }
                }
                search_re = es.search(index='short-video-production-2020', doc_type=monthly_doc_type_name,
                                      body=find_exist)
                if search_re['hits']['total'] == 0:
                # if True:
                    re_list.append(one_scan['_source'])
                else:
                    count_has += 1
            # print(releaser,platform,count_has)
            if len(re_list) > 0:
                print(count_has)
                print(releaser, platform,len(re_list))
                func_write_into_monthly_index_new_released(re_list, doc_type=monthly_doc_type_name)

# for releaser in miaopai_list:
#     print(releaser)
#     re_list = []
#     search_body = {
#                "query": {
#                    "bool": {
#                      "filter": [
#                        {"term": {"releaser_id_str": releaser}},
#                        {"range": {"release_time": {"gte": re_s_t,"lt":re_e_t}}},
#                      #  {"range": {"fetch_time": {"gte": 1556640000000}}}
#                        ]
#                    }
#                      }
#                      }
#     scan_re = scan(client=es, index='short-video-all-time-url', doc_type='all-time-url',
#                    query=search_body, scroll='3m')
#     count_has = 0
#     for one_scan in scan_re:
#         doc_id = cal_doc_id(one_scan["_source"]["platform"], url=one_scan["_source"]["url"],
#                            doc_id_type='all-time-url',
#                            data_dict=one_scan["_source"])
#         find_exist = {
#            "query": {
#                "bool": {
#                    "filter": [
#                        {"term": {"_id": doc_id}}
#                    ]
#                }
#            }
#         }
#         search_re = es.search(index='short-video-production-2019', doc_type=monthly_doc_type_name,
#                               body=find_exist)
#         if search_re['hits']['total'] == 0:
#             re_list.append(one_scan['_source'])
#         else:
#             count_has += 1
#             #print(count_has)
#     print(count_has)
#     print(len(re_list))
#     func_write_into_monthly_index_new_released(re_list, doc_type=monthly_doc_type_name)
