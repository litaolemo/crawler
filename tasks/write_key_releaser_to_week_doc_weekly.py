# -*- coding:utf-8 -*-
# @Time : 2019/8/14 18:01 
# @Author : litao


import json
# import argparse
import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from func_find_week_num import find_week_belongs_to
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler
from crawler.crawler_sys.utils import trans_format
from write_data_into_es.func_cal_doc_id import cal_doc_id

hosts = '192.168.17.11'
port = 80
user = 'zhouyujiang'
passwd = '8tM9JDN2LVxM'
http_auth = (user, passwd)

es = Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)


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
        weekly_cal_base = 'accumulate'
        timestamp = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)

        line.update({
                'timestamp': timestamp,
                'weekly_cal_base': weekly_cal_base,
                'weekly_net_inc_favorite_count': weekly_net_inc_favorite_count,
                'weekly_net_inc_comment_count': weekly_net_inc_comment_count,
                'weekly_net_inc_play_count': weekly_net_inc_play_count
        })
        re_list.append(line)
        url = line['url']
        platform = line['platform']
        doc_id = cal_doc_id(platform, url=url, doc_id_type='all-time-url',data_dict=line)
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
            print(count)

    if bulk_all_body != '':
        eror_dic = es.bulk(body=bulk_all_body,
                           index=index,
                           doc_type=doc_type,
                           request_timeout=200)
        if eror_dic['errors'] is True:
            print(eror_dic)



todayT = datetime.datetime.now()
# todayT=datetime.datetime(2019,2,5)
week_day_start = 1
# if args.week_str is None:
seven_days_ago_T = todayT - datetime.timedelta(days=7)
week_year, week_no, week_day = find_week_belongs_to(seven_days_ago_T,
                                                    week_day_start)
week_start = week_start_day(week_year, week_no, week_day)
re_s = week_start - datetime.timedelta(1)
re_s_dt = datetime.datetime.strptime(str(re_s), '%Y-%m-%d')
re_s_t = int(datetime.datetime.timestamp(re_s_dt) * 1000)

re_e = week_start + datetime.timedelta(6)
re_e_dt = datetime.datetime.strptime(str(re_e), '%Y-%m-%d')
re_e_t = int(datetime.datetime.timestamp(re_e_dt) * 1000)
#    nowT_feihua = week_start + datetime.timedelta(days=6)
weekly_doc_type_name = define_doc_type(week_year, week_no,
                                       week_day_start=week_day_start)
key_releaser_body = {
  "query": {
    "bool": {
      "filter": [
     {"term": {"key_releaser.keyword": "True"}}
        ]
    }
  }
}

releaser_re = scan(client=es, index='target_releasers', doc_type='doc',
                           query=key_releaser_body, scroll='3m')
for re in releaser_re:
    releaser = re["_source"]['releaser']
    platform = re["_source"]['platform']
    if releaser != None:
        re_list = []
        search_body = {
                "query": {
                        "bool": {
                                "filter": [
                                        {"term": {"platform.keyword": platform}},
                                        {"term": {"releaser.keyword": releaser}},
                                        {"range": {"release_time": {"gte": re_s_t, "lt": re_e_t}}},
                                        {"range": {"fetch_time": {"gte": re_s_t}}}

                                ]
                        }
                }
        }

        scan_re = scan(client=es, index='short-video-all-time-url', doc_type='all-time-url',
                       query=search_body, scroll='3m')
        for one_scan in scan_re:
            re_list.append(one_scan['_source'])
        func_write_into_weekly_index_new_released(re_list, doc_type=weekly_doc_type_name)


