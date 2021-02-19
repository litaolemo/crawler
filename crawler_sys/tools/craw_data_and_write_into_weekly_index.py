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
from func_find_week_num import find_week_belongs_to
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler
from crawler.crawler_sys.utils import trans_format
from func_cal_doc_id import cal_doc_id
from write_data_into_es.func_get_releaser_id import *


hosts = '192.168.17.11'
port = 80
user = 'zhouyujiang'
passwd = '8tM9JDN2LVxM'
http_auth = (user, passwd)

es = Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)
#parser = argparse.ArgumentParser()
#parser.add_argument('-w', '--week_str', type=str, default=None)

def week_start_day(week_year, week_no,week_day, week_day_start=1):
    year_week_start = find_first_day_for_given_start_weekday(week_year, week_day_start)
    week_start = year_week_start + datetime.timedelta(days=(week_no-1)*7)
    return week_start
def define_doc_type(week_year, week_no, week_day_start):
    """
    doc_type = 'daily-url-2018_w24_s2' means select Tuesday as the
    first day of each week, it's year 2018's 24th week.
    In isocalendar defination, Monday - weekday 1, Tuesday - weekday 2,
    ..., Saturday - weekday 6, Sunday -  weekday 7.
    """
    doc_type_str = 'daily-url-%d_w%02d_s%d' % (week_year, week_no, week_day_start)
    return doc_type_str 
    
def find_first_day_for_given_start_weekday(year, start_weekday):
    i = 0
    while i<7:
        dayDi = datetime.date(year, 1, 1) + datetime.timedelta(days=i)
        if dayDi.weekday()==start_weekday:
            cal_day1D = dayDi - datetime.timedelta(days=1)
            break
        else:
            cal_day1D = None
        i += 1
    return cal_day1D
    
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
def func_write_into_weekly_index_new_released(line_list, doc_type, index='short-video-weekly'):
    count = 0 
    bulk_all_body = ''
    re_list = []
    for line in line_list:
        count = count + 1 
        weekly_net_inc_play_count = line['play_count']
        weekly_net_inc_comment_count = line['comment_count']
        weekly_net_inc_favorite_count = line['favorite_count']
        try:
            weekly_net_inc_repost_count = line['repost_count']
        except:
            weekly_net_inc_repost_count = 0
        weekly_cal_base = 'accumulate'
        timestamp = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)

        line.update({
            'timestamp': timestamp,
            'weekly_cal_base': weekly_cal_base,
            'weekly_net_inc_favorite_count': weekly_net_inc_favorite_count,
            'weekly_net_inc_comment_count': weekly_net_inc_comment_count,
            'weekly_net_inc_play_count': weekly_net_inc_play_count,
            'weekly_net_inc_repost_count': weekly_net_inc_repost_count,
        })
        re_list.append(line)
        # if 'video_id' in line.keys():
        #     line.pop('video_id')
        url = line['url']

        platform = line['platform']
        #print(platform)
        doc_id = cal_doc_id(platform, url=url, doc_id_type='all-time-url',data_dict=line)
        #print(doc_id)
        bulk_head = '{"index": {"_id":"%s"}}' % doc_id
        data_str = json.dumps(line, ensure_ascii=False)

        bulk_one_body = bulk_head + '\n' + data_str + '\n'
        #
        bulk_all_body += bulk_one_body
        if count%500 == 0:
             eror_dic = es.bulk(index=index, doc_type=doc_type,
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
    
todayT = datetime.datetime.now()
#todayT=datetime.datetime(2019,2,5)
week_day_start = 1
#if args.week_str is None:
seven_days_ago_T = todayT - datetime.timedelta(days=9)
week_year, week_no, week_day = find_week_belongs_to(seven_days_ago_T,
                                                    week_day_start)
week_start = week_start_day(week_year,week_no,week_day)
re_s = week_start - datetime.timedelta(1)
re_s_dt = datetime.datetime.strptime(str(re_s), '%Y-%m-%d')
re_s_t = int(datetime.datetime.timestamp(re_s_dt)*1000)

re_e = week_start + datetime.timedelta(6)
re_e_dt = datetime.datetime.strptime(str(re_e), '%Y-%m-%d')
re_e_t = int(datetime.datetime.timestamp(re_e_dt)*1000)
#    nowT_feihua = week_start + datetime.timedelta(days=6)
weekly_doc_type_name = define_doc_type(week_year, week_no +1,
                                       week_day_start=week_day_start)
miaopai_list = []
# file = r'D:\work_file\发布者账号\anhui.csv'
file = r'D:\work_file\发布者账号\一次性需求附件\month10month9_releaser_video_num预警 2019-11-05.csv'
file = r'D:\work_file\发布者账号\周数据账号\周.csv'
with open(file, 'r',encoding="gb18030")as f:
    header_Lst = f.readline().strip().split(',')
    for line in f:
        count_has = 0
        line_Lst = line.strip().split(',')
        line_dict = dict(zip(header_Lst, line_Lst))
        releaser = line_dict['releaser']
        platform = line_dict['platform']
        # if platform != "haokan":
        #     continue
        try:
            releaserUrl = line_dict['releaserUrl']
        except:
            releaserUrl = func_search_reUrl_from_target_index(platform, releaser)
        if platform == 'miaopai':
            miaopai_list.append(releaser)
        releaserUrl = func_search_reUrl_from_target_index(platform, releaser)
        print(releaserUrl)
        line_dict["releaser_id"] = get_releaser_id(platform=platform, releaserUrl=releaserUrl)
        if line_dict["releaser_id"]:
            doc_id = line_dict['platform'] + '_' + line_dict['releaser_id']
        else:
            print("error_url ", releaserUrl)
        if releaserUrl != None:
            re_list = []
            # get_time = get_target_releaser_video_info(
            #                               platform=platform,
            #                               releaserUrl=releaserUrl,
            #                               releaser_page_num_max=200,
            #                               es_index='crawler-data-raw',
            #                               doc_type='doc'
            #                               )
            if platform != "kwai":
                continue
            search_body = {
            "query": {
                "bool": {
                  "filter": [
                   {"term": {"platform.keyword": platform}},
                   # {"term": {"platform.keyword": "toutiao"}},
                   {"term": {"releaser_id_str.keyword": doc_id}},
                          #{"range": {"release_time": {"gte": 1563638400000}}},
                    {"range": {"release_time": {"gte": re_s_t,"lt":re_e_t}}},
                    {"range": {"fetch_time": {"gte": re_s_t}}},

                    ]
                }
                  }
                   }

            scan_re = scan(client=es, index='crawler-data-raw', doc_type='doc',
                          query=search_body, scroll='3m')
            # scan_re = scan(client=es, index='short-video-all-time-url', doc_type='all-time-url',
            #                query=search_body, scroll='3m')
            for one_scan in scan_re:
                re_list.append(one_scan['_source'])
                count_has += 1
                if count_has % 500 == 0:
                    print(count_has)
                    print(len(re_list))
                    func_write_into_weekly_index_new_released(re_list, doc_type=weekly_doc_type_name)
                    re_list = []
            func_write_into_weekly_index_new_released(re_list, doc_type=weekly_doc_type_name)
        # break
# for releaser in miaopai_list:
#    re_list = []
#    search_body = {
#                "query": {
#                    "bool": {
#                      "filter": [
#                        {"term": {"platform.keyword": 'miaopai'}},
#                        {"term": {"releaser.keyword": releaser}},
#                        {"range": {"release_time": {"gte": re_s_t,"lt":re_e_t}}},
#
#                        ]
#                    }
#                      }
#                      }
#    scan_re = scan(client=es, index='short-video-all-time-url', doc_type='all-time-url',
#                    query=search_body, scroll='3m')
#    for one_scan in scan_re:
#        re_list.append(one_scan['_source'])
#    func_write_into_weekly_index_new_released(re_list, doc_type=weekly_doc_type_name)

