# -*- coding:utf-8 -*-
# @Time : 2019/4/24 9:33
# @Author : litao
# -*- coding: utf-8 -*-
# 新增发布者爬取该发布者的视频存入alltime库中

# import time
import json
# import argparse
import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from func_find_week_num import find_week_belongs_to
# from crawler.crawler_sys.framework.platform_crawler_register import get_crawler
from crawler.crawler_sys.utils import trans_format
from func_cal_doc_id import cal_doc_id
from crawler.crawler_sys.site_crawler_test import crawler_tudou
from crawler.crawler_sys.site_crawler_test import crawler_wangyi_news
from crawler.crawler_sys.site_crawler_test import crawler_haokan
from crawler.crawler_sys.site_crawler_test import crawler_toutiao
from crawler.crawler_sys.site_crawler_test import crawler_v_qq
from crawler.crawler_sys.site_crawler_test import crawler_tencent_news
hosts = '192.168.17.11'
port = 80
user = 'zhouyujiang'
passwd = '8tM9JDN2LVxM'
http_auth = (user, passwd)
from func_get_releaser_id import *
es = Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)


# es.bulk
# parser = argparse.ArgumentParser()
# parser.add_argument('-w', '--week_str', type=str, default=None)

def week_start_day(week_year, week_no, week_day, week_day_start=1):
    year_week_start = find_first_day_for_given_start_weekday(week_year, week_day_start)
    week_start = year_week_start + datetime.timedelta(days=(week_no - 1) * 7)
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
    while i < 7:
        dayDi = datetime.date(year, 1, 1) + datetime.timedelta(days=i)
        if dayDi.weekday() == start_weekday:
            cal_day1D = dayDi - datetime.timedelta(days=1)
            break
        else:
            cal_day1D = None
        i += 1
    return cal_day1D


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
            crawler_initialization.releaser_page_by_time(releaserUrl=releaserUrl,
                                                 releaser_page_num_max=releaser_page_num_max,
                                                 output_to_es_raw=True,
                                                 es_index=es_index,
                                                 doc_type=doc_type,
                                                 fetchFavoriteCommnt=True)
        except:
            print(releaserUrl, platform, file=log_file)
    else:
        try:
            crawler_initialization.releaser_page_by_time(releaserUrl=releaserUrl,
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


def func_write_into_alltime_index_new_released(line_list, doc_type, index='short-video-all-time-url'):
    count = 0
    bulk_all_body = ''
    re_list = []
    for line in line_list:
        count = count + 1
        re_list.append(line)
        # if 'video_id' in line.keys():
        #     line.pop('video_id')
        url = line['url']

        platform = line['platform']
        print(platform)
        # if platform == "腾讯新闻":
        #     line.pop("data_source")
        #     line["releaserUrl"] = line["playcnt_url"]
        doc_id = cal_doc_id(platform, url=url, doc_id_type='all-time-url',data_dict=line)
        # print(doc_id)
        bulk_head = '{"index": {"_id":"%s"}}' % doc_id
        data_str = json.dumps(line, ensure_ascii=False)

        bulk_one_body = bulk_head + '\n' + data_str + '\n'
        #
        bulk_all_body += bulk_one_body
        if count % 100 == 0:

            eror_dic = es.bulk(index=index, doc_type=doc_type,
                               body=bulk_all_body, request_timeout=200)
            bulk_all_body = ''
            if eror_dic['errors'] is True:
                print(eror_dic['items'])
                print(bulk_all_body)
            print(count)

    if bulk_all_body != '':
        eror_dic = es.bulk(body=bulk_all_body,
                           index=index,
                           doc_type=doc_type,
                           request_timeout=200)
        if eror_dic['errors'] is True:
            print(eror_dic)

platform_crawler_reg = {
    'toutiao': crawler_toutiao.Crawler_toutiao,
    '腾讯视频': crawler_v_qq.Crawler_v_qq,
    # 'iqiyi': crawler_iqiyi.Crawler_iqiyi,
    # 'youku': crawler_youku.Crawler_youku,
    'new_tudou': crawler_tudou.Crawler_tudou,
    'haokan': crawler_haokan.Crawler_haokan,
    '腾讯新闻': crawler_tencent_news.Crawler_Tencent_News,
    # 'miaopai': crawler_miaopai.Crawler_miaopai,
    # 'pearvideo': crawler_pear.Crawler_pear,
    # 'bilibili': crawler_bilibili.Crawler_bilibili,
    # 'Mango': crawler_mango,
    "网易新闻": crawler_wangyi_news.Crawler_wangyi_news
}


def get_crawler(platform):
    if platform in platform_crawler_reg:
        platform_crawler = platform_crawler_reg[platform]
    else:
        platform_crawler = None
        print("can't get crawler for platform %s, "
              "do we have the crawler for that platform?" % platform)
    return platform_crawler




miaopai_list = []
file = r'D:\work_file\5月补数据.csv'
es_index = 'crawler-data-raw'
doc_type = 'doc'
with open(file, 'r')as f:
    header_Lst = f.readline().strip().split(',')
    for line in f:
        line_Lst = line.strip().split(',')
        line_dict = dict(zip(header_Lst, line_Lst))
        releaser = line_dict['releaser']
        platform = line_dict['platform']
        releaserUrl = line_dict['releaserUrl']
        if platform == 'miaopai':
            miaopai_list.append(releaser)
        search_body = {
            "query": {
                "bool": {
                    "filter": [
                            {"term": {"platform.keyword": platform}},
                            # {"term": {"releaser.keyword": releaser}},
                            {"term": {"releaser_id_str": platform + "_" + get_releaser_id(platform=platform,
                                                                                          releaserUrl=releaserUrl)}},
                       # {"term": {"platform.keyword": "腾讯新闻"}},
                       # {"term": {"releaser.keyword": releaser}},
                        {"range": {"fetch_time": {"gte": 1562314519000}}}
                    ]
                }
            }
        }

        re_list = []
        if releaserUrl != None and platform in ["toutiao","new_tudou","腾讯视频","haokan","网易新闻"]:
            # pass
            get_time = get_target_releaser_video_info(
                platform=platform,
                releaserUrl=releaserUrl,
                releaser_page_num_max=300,
                es_index=es_index,
                doc_type=doc_type
            )


        scan_re = scan(client=es, index=es_index, doc_type=doc_type,
                       query=search_body, scroll='3m')
        for one_scan in scan_re:
            re_list.append(one_scan['_source'])
        func_write_into_alltime_index_new_released(re_list, doc_type="all-time-url")
        break
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
