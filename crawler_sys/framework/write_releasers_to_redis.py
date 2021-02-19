# -*- coding:utf-8 -*-
# @Time : 2020/4/24 14:15 
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

import sys
import argparse,copy
from crawler.crawler_sys.framework.es_target_releasers import get_releaserUrls_from_es
from crawler.crawler_sys.framework.platform_crawler_register import platform_crawler_reg
import redis,json


from redis.sentinel import Sentinel
sentinel = Sentinel([('192.168.17.65', 26379),
                     ('192.168.17.66', 26379),
                     ('192.168.17.67', 26379)
             ],socket_timeout=0.5)
# 查看master节点
master = sentinel.discover_master('ida_redis_master')
# 查看slave 节点
slave = sentinel.discover_slaves('ida_redis_master')
# 连接数据库
rds = sentinel.master_for('ida_redis_master', socket_timeout=0.5, db=1, decode_responses=True)
# rds = redis.StrictRedis(host='192.168.17.60', port=6379, db=1, decode_responses=True)

parser = argparse.ArgumentParser(description='Specify a platform name.')
parser.add_argument('-p', '--platform', default=[], action='append',
                    help=('Pass platform names, they will be assembled in python list.'))
parser.add_argument('-pj', '--project_tags', default=[], action='append',
                    help=('Pass platform names, they will be assembled in python list.'))
parser.add_argument('-n', '--max_page', default=2, type=int,
                    help=('The max page numbers to be scroll for each releaser url, '
                          'must be an int value, default to 30.'))
parser.add_argument('-fre', '--frequency', default=1, type=int,
                    help=('choose a frequency to retrieve releaserUrl,'
                          '1, 3 or 9 is legal number, default 1'))
parser.add_argument('-proxies', '--proxies', default=0, type=int,
                    help=('Crawler proxies_num'))
parser.add_argument('-d', '--date', default=3, type=int,
                    help=('Crawler backtracking data time'))
parser.add_argument('-s', '--processes_num', default=5, type=int,
                    help=('Processes number to be used in multiprocessing'))
parser.add_argument('-article', '--article', default=0, type=int,
                    help=('is article page'))
args = parser.parse_args()


if args.platform != []:
    platforms = args.platform
else:
    print('platform must be input')
    sys.exit(0)


releaser_page_num_max = args.max_page
frequency = args.frequency
if frequency == '':
    frequency = None

processes_num = args.processes_num
frequency = args.frequency
print(frequency)
if frequency == 0:
    frequency = None


kwargs_dict = {
        "proxies_num": 0,
        "date":args.date,
}
if frequency:
    if frequency >= 3:
        kwargs_dict["proxies_num"] = 3
if args.proxies:
    kwargs_dict["proxies_num"] = args.proxies
is_article = args.article

def write_project_to_redis(platform, data):
    rds.rpush(platform, data)


def write_releaserUrl_to_redis(data_dic):
    write_project_to_redis(data_dic["platform"], json.dumps(data_dic))


for platform in platforms:
    # 2 get releaserUrl list on each platform from target-releasers index
    releaserUrl_Lst = get_releaserUrls_from_es(platform=platform, frequency=frequency,target_index="target_releasers",project_tags=args.project_tags)
    if is_article:
        platform = platform + "_article"
    rds.hset("process_num",platform,processes_num)
    if releaserUrl_Lst == []:

        print('Get empty releaserUrl_Lst for platform %s' % platform)
        continue
    # 3 get crawler for this platform
    for releaserUrl,releaser in releaserUrl_Lst:
        push_dic = {
                "releaserUrl":releaserUrl,
                "releaser":releaser,
                "platform":platform,
        }
        push_dic.update(kwargs_dict)
        write_releaserUrl_to_redis(push_dic)
