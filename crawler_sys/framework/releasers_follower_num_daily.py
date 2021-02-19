# -*- coding:utf-8 -*-
# @Time : 2019/8/19 16:51 
# @Author : litao

import argparse, datetime, json, os
from multiprocessing import Pool
from crawler.crawler_sys.utils import trans_format
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import logging
from write_data_into_es.func_get_releaser_id import *

parser = argparse.ArgumentParser(description='get releaser follow number')
parser.add_argument('-p', '--process_num', default=10, help=('process num'),type=int)
parser.add_argument('-pl', '--platform', default=[], action='append',
                    help=('Pass platform names, they will be assembled in python list.'))
args = parser.parse_args()
processes_num = args.process_num

# es = Elasticsearch(hosts='192.168.17.11', port=80,
#                    http_auth=('crawler', 'XBcasfo8dgfs'))
hosts = '192.168.17.11'
port = 80
user = 'litao'
passwd = 'lQSmSEnGZZxl'
http_auth = (user, passwd)
es = Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)
write_index = "releaser_fans"
write_type = "doc"


def bulk_to_es(single_data):
    bulk_all_body = ""
    error_info = ""
    try:
        _id = single_data["releaser_id_str"] + "_" + str(int(
                datetime.datetime(year=single_data["data_year"], month=single_data["data_month"],
                                  day=single_data["data_day"]).timestamp() * 1e3))
    except:
        releaser_id = get_releaser_id(platform=single_data['platform'], releaserUrl=single_data[
            'releaserUrl'])
        single_data['releaser_id_str'] = single_data['platform'] + "_" + releaser_id
        _id = single_data["releaser_id_str"] + "_" + str(int(
                datetime.datetime(year=single_data["data_year"], month=single_data["data_month"],
                                  day=single_data["data_day"]).timestamp() * 1e3))

    bulk_head = '{"index": {"_id":"%s"}}' % _id
    bulk_body = json.dumps(single_data, ensure_ascii=False)
    bulk_one_body = bulk_head + '\n' + bulk_body + '\n'
    bulk_all_body += bulk_one_body

    eror_dic = es.bulk(index=write_index, doc_type=write_type,
                       body=bulk_all_body, request_timeout=200)
    bulk_all_body = ''
    if eror_dic['errors'] is True:
        count_false = 1
        print(eror_dic['items'])
        print(bulk_all_body)
        error_info += eror_dic['items']


def write_fans_monthly():
    now = datetime.datetime.now()
    print(now.day)
    bulk_all_body = ""
    error_info = ""
    if now.day == 1:
        this_mon = now.month
        this_year = now.year
        last_month = this_mon - 1 if this_mon >= 2 else 12
        last_year = this_year if last_month != 12 else this_year - 1
        search_body = {
                "query": {
                        "bool": {
                                "filter": [
                                        {"term": {"data_year": last_year}},
                                        {"term": {"data_month": last_month}},
                                ], "must": [{"exists": {"field": "releaser_followers_count"}}]
                        }}
        }
        res_scan = scan(client=es, query=search_body, index="releaser_fans_monthly", doc_type="doc")
        for count_true,data in enumerate(res_scan):
            data["_source"]["data_month"] = this_mon
            data["_source"]["data_year"] = this_year
            if data["_source"]["platform"] == "weibo":
                data["_source"]["UID"] = data["_source"]["releaser_id_str"]
            try:
                _id = data["_source"]["releaser_id_str"] + "_" + str(int(
                            datetime.datetime(year=this_year, month=this_mon,
                                              day=1).timestamp() * 1e3))
            except:
                _id = data["_source"]["releaser"] + "_" + str(int(
                        datetime.datetime(year=this_year, month=this_mon,
                                          day=1).timestamp() * 1e3))

            bulk_head = '{"index": {"_id":"%s"}}' % _id
            bulk_body = json.dumps(data["_source"], ensure_ascii=False)
            bulk_one_body = bulk_head + '\n' + bulk_body + '\n'
            bulk_all_body += bulk_one_body
            count_true += 1
            if count_true % 500 == 0:
                eror_dic = es.bulk(index="releaser_fans_monthly", doc_type="doc",
                                   body=bulk_all_body, request_timeout=200)
                if eror_dic['errors'] is True:
                    count_false = 1
                    print(eror_dic['items'])
                    print(bulk_all_body)
                    error_info += eror_dic['items']
                print(count_true)
                bulk_all_body = ''

        if bulk_all_body != '':
            eror_dic = es.bulk(index="releaser_fans_monthly", doc_type="doc",
                               body=bulk_all_body, request_timeout=200)
            if eror_dic['errors'] is True:
                count_false = 1
                print(eror_dic)
                error_info += eror_dic['items']
            print(count_true)


def write_fans_to_target_doc(read_index=None, read_doc=None, write_index=None, write_doc=None, platform_list=None,
                             day=15, all_time_doc_id=False, monthly_doc_id=False,scroll="50m"):
    count_true = 0
    error_info = ""
    bulk_all_body = ""
    douyin_search_body = {
            "query": {
                    "bool": {
                            "filter": [
                                    # {"term": {"platform.keyword": "kwai"}},
                                    # {"term": {"releaser_id_str": "kwai_3xcurnwdxabnn5w"}},
                                    {"range": {"fetch_time": {"gte": int(
                                        (datetime.datetime.now() - datetime.timedelta(days=day)).timestamp() * 1e3)}}}
                            ], "must": [{"exists": {"field": "releaser_followers_count"}}]
                    }},"sort": [
    {
      "fetch_time": {
        "order": "asc"
      }
    }
  ]
    }
    if platform_list:
        douyin_search_body["query"]["bool"]["filter"].append({"terms": {"platform.keyword": platform_list}})
    douyin_seacn = scan(client=es, query=douyin_search_body, index=read_index, doc_type=read_doc,preserve_order=True,scroll=scroll)
    for single_res in douyin_seacn:
        try:
            releaser = single_res["_source"]["releaser"]
            platform = single_res["_source"]["platform"]
            releaserUrl = single_res["_source"].get("releaserUrl")
            releaser_id_str = single_res["_source"].get("releaser_id_str")
            releaser_followers_count = int(single_res["_source"].get("releaser_followers_count"))
            timestamp = int(datetime.datetime.now().timestamp() * 1e3)
            fetch_time = single_res["_source"]["fetch_time"]
            now = datetime.datetime.fromtimestamp(fetch_time / 1000)
        except Exception as e:
            print(e)
            continue
        data_day = now.day
        data_month = now.month
        data_year = now.year
        bulk_dic = {
                "releaser": releaser,
                "platform": platform,
                "releaserUrl": releaserUrl,
                "releaser_id_str": releaser_id_str,
                "releaser_followers_count": releaser_followers_count,
                "timestamp": timestamp,
                "fetch_time": fetch_time,
                "data_day": data_day,
                "data_month": data_month,
                "data_year": data_year
        }
        if all_time_doc_id:
            _id = releaser_id_str
        elif monthly_doc_id:
            if not releaser_id_str:
                _id = releaser + "_" + str(int(
                        datetime.datetime(year=bulk_dic["data_year"], month=bulk_dic["data_month"],
                                          day=1).timestamp() * 1e3))
            else:
                _id = releaser_id_str + "_" + str(int(
                        datetime.datetime(year=data_year, month=data_month,
                                          day=1).timestamp() * 1e3))
        else:
            if not releaser_id_str:
                _id = releaser + "_" + str(int(
                        datetime.datetime(year=bulk_dic["data_year"], month=bulk_dic["data_month"],
                                          day=bulk_dic["data_day"]).timestamp() * 1e3))
            else:
                _id = releaser_id_str + "_" + str(int(
                        datetime.datetime(year=data_year, month=data_month,
                                          day=data_day).timestamp() * 1e3))
            if not _id:
                continue
        if bulk_dic["platform"] == "weibo":
            bulk_dic["UID"] = bulk_dic["releaser_id_str"]
        bulk_head = '{"index": {"_id":"%s"}}' % _id
        bulk_body = json.dumps(bulk_dic, ensure_ascii=False)
        bulk_one_body = bulk_head + '\n' + bulk_body + '\n'
        bulk_all_body += bulk_one_body
        count_true += 1
        if count_true % 1000 == 0:
            eror_dic = es.bulk(index=write_index, doc_type=write_doc,
                               body=bulk_all_body, request_timeout=200)
            if eror_dic['errors'] is True:
                count_false = 1
                print(eror_dic['items'])
                print(bulk_all_body)
            print(count_true)
            bulk_all_body = ''

    if bulk_all_body != '':
        eror_dic = es.bulk(index=write_index, doc_type=write_doc,
                           body=bulk_all_body, request_timeout=200)
        if eror_dic['errors'] is True:
            count_false = 1
            print(eror_dic)
        print(count_true)


def weibo_fans():
    pass


def get_target_releasers(platform=None):
    releasers_dic = {}
    search_body = {
            "query": {
                    "bool": {
                            "filter": [
                                   # {"term": {"platform.keyword": "kwai"}}
                            ], "must": [{"exists": {"field": "releaser_id_str"}}]
                    }
            }
    }
    if platform:
        search_body["query"]["bool"]["filter"].append({"terms": {"platform.keyword": platform}})
    res_scan = scan(client=es, query=search_body, index="target_releasers", doc_type="doc")
    for res in res_scan:
        try:
            res["_source"].pop("is_purchased",0)
        except:
            pass
        if res["_source"].get("platform") in releasers_dic:
            releasers_dic[res["_source"].get("platform")].append(res["_source"])
        else:
            releasers_dic[res["_source"].get("platform")] = []
            releasers_dic[res["_source"].get("platform")].append(res["_source"])
    return releasers_dic


def get_releaser_follower_num(line, get_crawler):
    releaserUrl = line['releaserUrl']
    platform = line['platform']
    crawler_initialization = get_crawler(platform)
    try:
        now = datetime.datetime.now()
        crawler = crawler_initialization().get_releaser_follower_num
        follower_num, releaser_img = crawler(releaserUrl)
        line['releaser_followers_count'] = follower_num
        line['releaser_img'] = releaser_img
        line["fetch_time"] = int(now.timestamp() * 1e3)
        line["data_day"] = now.day
        line["data_month"] = now.month
        line["data_year"] = now.year
        # print(line['releaserUrl'], line['platform'], line['releaser_followers_count'],releaser_img)
        if line['releaser_followers_count'] is None:
            return None
        bulk_to_es(line)
    except Exception as e:
        print(e)
        print(platform + " " + releaserUrl + ", faile to get fans num")


if __name__ == "__main__":
    try:
        if not args.platform:
            write_fans_monthly()
            write_fans_to_target_doc(read_index="short-video-production", read_doc="daily-url",
                                     write_index=write_index, write_doc=write_type,
                                     platform_list=["抖音", "haokan", "腾讯新闻","kwai","miaopai"], day=4)
            write_fans_to_target_doc(read_index="releaser_fans", read_doc="doc", write_index="releaser_fans_monthly",
                                     write_doc="doc", day=7, monthly_doc_id=True)
            write_fans_to_target_doc(read_index="releaser_fans", read_doc="doc", write_index="releaser_fans_latest",
                                     write_doc="doc", day=7, all_time_doc_id=True)

    except:
        pass
    try:
        pool = Pool(processes=processes_num)
        releasers_dic = get_target_releasers(platform=args.platform)
        print("get data")
        while releasers_dic:
            for platform in releasers_dic:

                if len(releasers_dic[platform]) == 0:
                    releasers_dic.pop(platform,0)
                    break
                line = releasers_dic[platform].pop()
                    # print(line['releaserUrl'], line['platform'])
                    # get_releaser_follower_num(line, get_crawler)
                pool.apply_async(func=get_releaser_follower_num, args=(line, get_crawler))

        pool.close()
        pool.join()
    except:
        pass
    finally:
        write_fans_monthly()
        write_fans_to_target_doc(read_index="releaser_fans", read_doc="doc", write_index="releaser_fans_latest",
                                 write_doc="doc", day=5, all_time_doc_id=True)
        write_fans_to_target_doc(read_index="releaser_fans", read_doc="doc", write_index="releaser_fans_monthly",
                                 write_doc="doc", day=10, monthly_doc_id=True)
    # write_fans_monthly()
    # write_fans_to_target_doc(read_index="releaser_fans", read_doc="doc", write_index="releaser_fans_monthly",
    #                          write_doc="doc", day=10, monthly_doc_id=True)

    # releaser_dic = get_target_releasers()
    # for platforn in releaser_dic:
    #     for line in releaser_dic[platforn]:
    #         get_releaser_follower_num(line,get_crawler)
    # fn = r"D:\work_file\5月补数据.csv"
    # with open(fn, 'r', encoding='gb18030')as f:
    #     head = f.readline()
    #     head_list = head.strip().split(',')
    #     for i in f:
    #         print("\n")
    #         line_dict = {}
    #         line_list = i.strip().split(',')
    #         test_dict = dict(zip(head_list, line_list))
    #         get_releaser_follower_num(test_dict, get_crawler)
    # write_fans_to_target_doc(read_index="short-video-daily-url-2019", read_doc="daily-url", write_index=write_index,
    #                          write_doc=write_type, platform_list=["抖音", "haokan", "腾讯新闻"], day=8)
    # write_fans_to_target_doc(read_index="releaser_fans", read_doc="doc", write_index="short_video_fans_latest",
    #                          write_doc="doc", day=6, all_time_doc_id=True)
