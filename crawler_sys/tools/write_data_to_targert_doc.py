# -*- coding:utf-8 -*-
# @Time : 2019/9/26 15:00 
# @Author : litao


import json,time
import datetime, redis
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from write_data_into_es.func_cal_doc_id import cal_doc_id
from crawler.crawler_sys.framework.func_get_doc_name_and_timestamp import *
from maintenance.send_email_with_file_auto_task import write_email_task_to_redis
from write_data_into_es.func_get_releaser_id import get_releaser_id

hosts = '192.168.17.11'
port = 80
user = 'zhouyujiang'
passwd = '8tM9JDN2LVxM'
http_auth = (user, passwd)
rds = redis.StrictRedis(host='192.168.17.60', port=6379, db=2, decode_responses=True)
es = Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)


def get_status():
    res_dic = {}
    len_status = rds.llen("write_task")
    if len_status != 0:
        res_down_task = rds.lrange("write_task", 0, -1)
        for task_name in res_down_task:
            res = rds.hgetall(task_name)
            res_dic[task_name] = res
            rds.delete(task_name)
            rds.lpop("write_task")
        return res_dic
    else:
        return None


def write_status_to_redis(task_name, res_dic):
    rds.hmset(task_name, res_dic)
    rds.rpush("write_task", task_name)
    res = rds.hgetall(task_name)
    if res:
        return True
    else:
        return False


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


def func_write_into_target_index(line_list, doc_type, index, cycle):
    count = 0
    bulk_all_body = ''
    re_list = []
    for line in line_list:
        count = count + 1
        re_list.append(line)
        url = line['url']
        platform = line['platform']
        doc_id = cal_doc_id(platform, url=url, doc_id_type='all-time-url', data_dict=line)
        bulk_head = '{"index": {"_id":"%s"}}' % doc_id
        timestamp = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        if cycle == "month":
            line.update({
                    'timestamp': timestamp,
                    'monthly_cal_base': 'accumulate',
                    'monthly_net_inc_favorite_count': line['favorite_count'],
                    'monthly_net_inc_comment_count': line['comment_count'],
                    'monthly_net_inc_play_count': line['play_count']
            })
        elif cycle == "week":
            line.update({
                    'timestamp': timestamp,
                    'weekly_cal_base': "accumulate",
                    'weekly_net_inc_favorite_count': line['favorite_count'],
                    'weekly_net_inc_comment_count': line['comment_count'],
                    'weekly_net_inc_play_count': line['play_count']
            })
        data_str = json.dumps(line, ensure_ascii=False)
        bulk_one_body = bulk_head + '\n' + data_str + '\n'
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


def get_releaser_from_redis(one_project_name):
    one_project_dic = rds.get(one_project_name)
    data = json.loads(one_project_dic)
    write_data_to_target_doc(data.get("task_name"), cycle=data.get("cycle")
                             , cycle_num=data.get("cycle_num"), year=data.get("year"),
                             start_timestamp=data.get("start_timestamp"),
                             start_ts=data.get("start_ts"),end_ts=data.get("end_ts"), releaser_dic=data.get("releaser_dic"),
                             doc_type=data.get("doc_type"),sender=data.get("sender"))


def get_task_from_redis():
    res = rds.llen("write_task_list")
    one_project_name = ""
    while True:
        try:
            res = rds.llen("write_task_list")
            if res != 0:
                one_project_name = rds.lpop("write_task_list")
                get_releaser_from_redis(one_project_name)
                rds.delete(one_project_name)
            else:
                time.sleep(5)
                now = datetime.datetime.now()
                print(now.strftime("%Y-%m-%d %H:%M:%S"))
                # print("time sleep 5")
        except:
            print("task %s error") % one_project_name
            rds.delete(one_project_name)

def put_write_task_to_redis(file, task_name, cycle=None, cycle_num=None, year=None,
                            doc_type="crawler", start_timestamp=0, start_ts=0,end_ts=0,sender=None):
    """

    :param file: str 文件路径
    :param task_name: str 任务名
    :param cycle: str 写入切片  week/month/quarter/all-time
    :param cycle_num: int 月号/周号 1/2/3/4/5/6/7/8/9
    :param year: int 年份 2019
    :param doc_type: str 从哪个切片读取数据  crawler/all-time
    :param start_timestamp: int 开始抓取时间戳
    :param start_ts: int 写入数据日期开始时间戳  可不传
    :param end_ts: int 写入数据日期结束时间戳  可不传
    :param sender: str 收件人名称
    :return:
    """
    releaser_dic = {}
    with open(file, 'r', encoding="gb18030")as f:
        header_Lst = f.readline().strip().split(',')
        for line in f:
            line_Lst = line.strip().split(',')
            line_dict = dict(zip(header_Lst, line_Lst))
            releaser = line_dict.get('releaser')
            platform = line_dict.get('platform')
            try:
                releaserUrl = line_dict['releaserUrl']
            except:
                releaserUrl = func_search_reUrl_from_target_index(platform, releaser)
            releaser_id = get_releaser_id(platform=platform, releaserUrl=releaserUrl)
            if releaser_id:
                releaser_id_str = platform + "_" + releaser_id
            else:
                releaser_id_str = platform + "_" + releaser
            releaser_dic[releaser_id_str] = [platform, releaser, releaserUrl]

    put_dic = {
            "file": file,
            "task_name": task_name,
            "cycle": cycle,
            "cycle_num": cycle_num,
            "year": year,
            "doc_type": doc_type,
            "start_timestamp": start_timestamp,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "releaser_dic": releaser_dic,
            "sender":sender
    }
    rds.set("write_task_%s" % task_name, json.dumps(put_dic))
    rds.lpush("write_task_list", "write_task_%s" % task_name)
    return True


def write_data_to_target_doc(task_name, cycle=None, cycle_num=None, year=None, start_timestamp=0,
                             releaser_dic=None,
                             doc_type="crawler",start_ts=0,end_ts=0,sender=None, **kwargs):
    # 如果需要从all-time补充数据,请把doc_type传入all-time
    this_cycle_index, this_cycle_doc, fisrt_day_ts, last_day_ts = func_get_doc_and_timestmp(cycle=cycle,
                                                                                            cycle_num=cycle_num,
                                                                                            year=year)
    now = datetime.datetime.now()
    start_timestamp = int(datetime.datetime(year=now.year,month=now.month,day=now.day).timestamp()*1e3)
    if start_ts and end_ts and cycle == "all-time":
        fisrt_day_ts = start_ts
        last_day_ts = end_ts
    res_dic = {}
    if True:
        for releaser_id in releaser_dic:
            count_has = 0
            line_dict = releaser_dic[releaser_id]
            platform, releaser , releaserUrl = line_dict
            print(releaser, platform, releaserUrl)
            if releaserUrl != None:
                re_list = []
                search_body = {
                        "query": {
                                "bool": {
                                        "filter": [
                                                {"term": {"platform.keyword": platform}},
                                                {"term": {"releaser.keyword": releaser}},
                                                {"range": {"release_time": {"gte": fisrt_day_ts, "lt": last_day_ts}}},
                                                {"range": {"fetch_time": {"gte": start_timestamp}}}
                                        ]
                                }
                        }
                }
                if not fisrt_day_ts or not last_day_ts:
                    search_body["query"]["bool"]["filter"].pop(2)
                releaser_id_str = get_releaser_id(platform=platform, releaserUrl=releaserUrl)
                if releaser_id_str:
                    releaser_id_str = platform + '_' + releaser_id_str
                if releaser_id_str:
                    search_body["query"]["bool"]["filter"].pop(1)
                    if doc_type == "crawler":
                        search_body["query"]["bool"]["filter"].append({"term": {"releaser_id_str.keyword": releaser_id_str}})
                    else:
                        search_body["query"]["bool"]["filter"].append({"term": {"releaser_id_str": releaser_id_str}})
                else:
                    pass
                if doc_type == "crawler":
                    scan_re = scan(client=es, index='crawler-data-raw', doc_type='doc', query=search_body, scroll='3m')
                else:
                    scan_re = scan(client=es, index='short-video-all-time-url', doc_type='all-time-url',
                                   query=search_body,
                                   scroll='3m')
                for one_scan in scan_re:
                    # doc_id = cal_doc_id(one_scan["_source"]["platform"], url=one_scan["_source"]["url"],
                    #                     doc_id_type='all-time-url', data_dict=one_scan["_source"])
                    # find_exist = {
                    #         "query": {
                    #                 "bool": {
                    #                         "filter": [
                    #                                 {"term": {"_id": doc_id}}
                    #                         ]
                    #                 }
                    #         }
                    # }
                    # search_re = es.search(index=this_cycle_index, doc_type=this_cycle_doc,
                    #                       body=find_exist)
                    # if search_re['hits']['total'] >= 1:
                    re_list.append(one_scan['_source'])
                    count_has += 1
                    if count_has % 500 == 0:
                        print(count_has)
                        print(len(re_list))
                        func_write_into_target_index(re_list, this_cycle_doc, this_cycle_index, cycle)
                        re_list = []
                print(count_has)
                print(len(re_list))
                func_write_into_target_index(re_list, this_cycle_doc, this_cycle_index, cycle)
                res_dic[releaser + "/" + platform] = count_has

    res_dic["timestamp"] = int(datetime.datetime.now().timestamp() * 1e3)
    get_res = write_status_to_redis(task_name, res_dic)
    data_str = ""
    for one_line in res_dic:
        if one_line != "timestamp":
            data_str += one_line + "写入 %s 条数据\n" % res_dic[one_line]
    write_email_task_to_redis(task_name=task_name,sender=sender,cc_group=["gengdi@csm.com.cn","litao@csm.com.cn","liushuangdan@csm.com.cn"],
                              title_str="写入任务 %s 已完成" % task_name,email_msg_body_str=data_str,email_group=[sender]
                              )
    return get_res


if __name__ == "__main__":
    # 写入任务调用  put_write_task_to_redis
    # 如需自定义传入开始日期和结束日期
    """
      :param file: str 文件路径
      :param task_name: str 任务名
      :param cycle: str 写入切片  week/month/quarter/all-time
      :param cycle_num: int 月号/周号 1/2/3/4/5/6/7/8/9
      :param year: int 年份 2019
      :param doc_type: str 从哪个切片读取数据  crawler/all-time
      :param start_timestamp: int 开始抓取时间戳
      :param start_ts: int 写入数据日期开始时间戳  可不传
      :param end_ts: int 写入数据日期结束时间戳  可不传
      :param sender: str 收件人名称
      :return: boolean True or False
      """
    # print(get_status())
    get_task_from_redis()