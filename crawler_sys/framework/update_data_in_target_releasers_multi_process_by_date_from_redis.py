# -*- coding:utf-8 -*-
# @Time : 2020/4/24 15:21 
# @Author : litao
# -*- coding: utf-8 -*-
"""
Created on Mon May 14 17:52:02 2018

Find urls in given releaser page, and write first batch data into es.
Everytime this program runs, two things will happen:
1 All video urls in given releaser page will be fetched and put into redis url pool,
2 All data related to 1 will be fetched and stored into es.

Data in es will be update when run this program once.

@author: hanye
"""
from crawler.crawler_sys.site_crawler_by_redis import (crawler_toutiao, crawler_v_qq, crawler_tudou, crawler_haokan,
                                                       crawler_tencent_news,
                                                       crawler_wangyi_news, crawler_kwai, crawler_douyin,toutiao_article)
import sys
from crawler.crawler_sys.utils.output_results import output_result
import argparse, copy, datetime, time
from crawler.crawler_sys.framework.es_target_releasers import get_releaserUrls_from_es
from crawler.crawler_sys.utils.parse_bool_for_args import parse_bool_for_args
import redis,json
from concurrent.futures import ProcessPoolExecutor
import threading
from redis.sentinel import Sentinel

sentinel = Sentinel([('192.168.17.65', 26379),
                     ('192.168.17.66', 26379),
                     ('192.168.17.67', 26379)
                     ], socket_timeout=1)
# 查看master节点
master = sentinel.discover_master('ida_redis_master')
# 查看slave 节点
slave = sentinel.discover_slaves('ida_redis_master')
# 连接数据库
rds_1 = sentinel.master_for('ida_redis_master', socket_timeout=1, db=1, decode_responses=True)

# rds_1 = redis.StrictRedis(host='192.168.17.60', port=6379, db=1, decode_responses=True)

parser = argparse.ArgumentParser(description='Specify a platform name.')
parser.add_argument('-n', '--max_page', default=30, type=int,
                    help=('The max page numbers to be scroll for each releaser url, '
                          'must be an int value, default to 30.'))
parser.add_argument('-f', '--output_file_path', default='', type=str,
                    help=('Specify output file path, default None.'))
parser.add_argument('-r', '--push_to_redis', default='False', type=str,
                    help=('Write urls to redis or not, default to True'))
parser.add_argument('-w', '--output_to_es_raw', default='True', type=str,
                    help=('Write data into es or not, default to True'))
parser.add_argument('-index', '--es_index', default='crawler-data-raw', type=str,
                    help=('assign a es_index to write into, default to crawler-data-raw'))
parser.add_argument('-doc', '--doc_type', default='doc', type=str,
                    help=('assign a doc to write into, default to doc'))
parser.add_argument('-g', '--output_to_es_register', default='True', type=str,
                    help=('Write data into es or not, default to True'))
parser.add_argument('-processes', '--processes_num', default=8, type=int,
                    help=('Processes number to be used in multiprocessing'))
parser.add_argument('-exit_hour', '--exit_hour', default=22, type=int,
                    help=('Processes number to be used in multiprocessing'))
parser.add_argument('-thead', '--thead_num', default=5, type=int,
                    help=('thead_num number to be used in multiprocessing'))
parser.add_argument('-v', '--video', default="False", type=str,
                    help=('Is or not run video_page_crawler'))
parser.add_argument('-name', '--name', default="crawler01", type=str,
                    help=('this computer name'))
args = parser.parse_args()

platform_crawler_reg = {
        'toutiao': crawler_toutiao.Crawler_toutiao,
        'toutiao_article': toutiao_article.Crawler_toutiao_article,
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
        '抖音': crawler_douyin.Crawler_douyin,
        "网易新闻": crawler_wangyi_news.Crawler_wangyi_news,
        "kwai": crawler_kwai.Crawler_kwai
}


def get_crawler(platform):
    if platform in platform_crawler_reg:
        platform_crawler = platform_crawler_reg[platform]
    else:
        platform_crawler = None
        print("can't get crawler for platform %s, "
              "do we have the crawler for that platform?" % platform)
    return platform_crawler


releaser_page_num_max = args.max_page
output_f_path = args.output_file_path
exit_hour = args.exit_hour


if output_f_path == '':
    output_f_path = None

if output_f_path is None:
    output_to_file = False
else:
    output_to_file = True

push_to_redis = parse_bool_for_args(args.push_to_redis)
output_to_es_raw = parse_bool_for_args(args.output_to_es_raw)
output_to_es_register = parse_bool_for_args(args.output_to_es_register)

processes_num = args.processes_num
name = args.name
lock = threading.Lock()

es_index = args.es_index
doc_type = args.doc_type
end_time = int(datetime.datetime.now().timestamp() * 1e3)
kwargs_dict = {
        'output_to_file': output_to_file,
        'filepath': output_f_path,
        'releaser_page_num_max': releaser_page_num_max,
        'output_to_es_raw': output_to_es_raw,
        'es_index': es_index,
        'doc_type': doc_type,
        'output_to_es_register': output_to_es_register,
        'push_to_redis': push_to_redis,
}


def get_task_from_redis():
    while True:
        try:
            now = datetime.datetime.now()
            if now.hour >= exit_hour:
                sys.exit()
            res = rds_1.hgetall("process_num")
            for platform in platform_crawler_reg:
                platform_str = platform + "_process"
                try:
                    process_num = rds_1.get(platform_str)
                except:
                    continue
                try:
                    if rds_1.llen(platform) > 0:
                        if process_num:
                            if res.get(platform):
                                if int(res.get(platform)) > int(process_num):
                                    rds_1.incr(platform_str)
                                    yield platform
                                    rds_1.decr(platform_str)
                except Exception as e:
                    print(e)

        except:
            continue


def single_thead(processe,name):
    allow = 10
    data_list = []
    while True:
        now = datetime.datetime.now()
        if now.hour >= exit_hour:
            sys.exit()
        end_time = int(now.timestamp() * 1e3)
        for count, platform in enumerate(get_task_from_redis()):
            now = datetime.datetime.now()
            try:
                count_false = 0
                releaser_dic_str = rds_1.blpop(platform)
                releaser_body = json.loads(releaser_dic_str[1])
                start_time = int((datetime.datetime.now() + datetime.timedelta(days=-releaser_body["date"])).timestamp() * 1e3)
                count_has = False
                proxies_num = releaser_body["proxies_num"]
                try:
                    crawler = get_crawler(platform)()
                    for single_data in crawler.releaser_page_by_time(start_time=start_time,
                                                                     end_time=int(now.timestamp() * 1e3), url=releaser_body["releaserUrl"],
                                                                     allow=allow,**kwargs_dict,proxies_num=proxies_num):
                        count_has = True
                        video_time = single_data.get("release_time")
                        if video_time:
                            if start_time < video_time:
                                if video_time < end_time:
                                    data_list.append(single_data)
                            else:
                                count_false += 1
                                if count_false > allow*3:
                                    break
                                else:
                                    data_list.append(single_data)
                        if len(data_list) >= 100:
                            output_result(result_Lst=data_list,
                                          platform=platform,
                                          output_to_file=output_to_file,
                                          filepath=None,
                                          output_to_es_raw=output_to_es_raw,
                                          es_index=es_index,
                                          doc_type=doc_type,
                                          output_to_es_register=output_to_es_register)
                            print(len(data_list))
                            data_list.clear()

                    print("processe"+ str(processe) + " " +threading.current_thread().name + " down " + platform + str(count))
                    if not count_has:
                        releaser_body["mssage"] = "爬取失败,请检查账号"
                        rds_1.hset("error",releaser_body["platform"] + "/" +releaser_body["releaserUrl"],json.dumps(releaser_body))
                    if data_list != []:
                        output_result(result_Lst=data_list,
                                      platform=platform,
                                      output_to_file=output_to_file,
                                      filepath=None,
                                      output_to_es_raw=output_to_es_raw,
                                      es_index=es_index,
                                      doc_type=doc_type,
                                      output_to_es_register=output_to_es_register)
                        print(len(data_list))
                        data_list.clear()

                except Exception as e:
                    print(e)
                    releaser_body["mssage"] = "报错 %s " % e
                    rds_1.hset("error","%s_error"% platform,json.dumps(releaser_body))
            except Exception as e:
                print("253,",e)




# def singel_thead(processe):
#     global lock
#     while True:
#         lock.acquire()
#         print("processe "+ str(processe) + " " + threading.current_thread().name + ' test')
#         lock.release()


def start_crawler(processe,name):
    for count in range(args.thead_num):
        # single_thead(processe,name)
        t = threading.Thread(target=single_thead, name=str(count),args=(str(processe),name))
        # # t.setDaemon(False) #
        t.start()

if __name__ == "__main__":
    executor = ProcessPoolExecutor(max_workers=processes_num)
    futures = []
    for platform in platform_crawler_reg:
        platform_str = platform + "_process"
        rds_1.set(platform_str, 0)
    for processe in range(processes_num):
        # start_crawler(processe,name)
        # print(kwargs_dict)
        future = executor.submit(start_crawler,processe,name)
        futures.append(future)
        print('Processe %s start' % processe)
    while True:
        now = datetime.datetime.now()
        if now.hour >= exit_hour:
            for future in futures:
                future.cancel()
            sys.exit()
    # singel_thead("1")
