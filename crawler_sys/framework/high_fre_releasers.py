# -*- coding: utf-8 -*-
"""
Created on Mon Dec 10 17:07:09 2018

@author: fangyucheng
"""

import sys
import argparse
import configparser
from multiprocessing import Pool
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler
from crawler.crawler_sys.framework.platform_crawler_register import platform_crawler_reg

parser = argparse.ArgumentParser(description='a special crawler framework for key customer')
parser.add_argument('-p', '--platform', default=[], action='append',
                    help=('legal platform name is required'))
parser.add_argument('-c', '--conf', default='/home/hanye/crawlersNew/crawler/crawler_sys/framework/config/high_fre.ini',
                    help=('absolute path of config file'))
parser.add_argument('-num', '--page_num', default=20, type=int,
                    help=('the number of scrolling page'))
args = parser.parse_args()

if args.platform != []:
    platform_list = args.platform
for platform in platform_list:
    if platform not in platform_crawler_reg:
        print("%s is not a legal platform name" % platform)
        sys.exit(0)

config_file_path = args.conf
config = configparser.ConfigParser()
config.sections()
config.read(config_file_path)

releaser_page_num_max = args.page_num

ARGS_DICT = {"releaser_page_num_max": releaser_page_num_max,
             "output_to_es_raw": True,
             "output_es_index": "crawler-data-raw",
             "output_doc_type": "doc",
             "output_to_es_register": True}

for platform in platform_list:
    crawler_initialization = get_crawler(platform)
    crawler = crawler_initialization().releaser_page
    get_task_list = config[platform]
    TASK_LIST = []
    for key, value in get_task_list.items():
        TASK_LIST.append(value)
    pool = Pool(processes=20)
    for releaserUrl in TASK_LIST:
        pool.apply_async(func=crawler, args=(releaserUrl,), kwds=ARGS_DICT)
    pool.close()
    pool.join()
    print('Multiprocessing done for platform %s' % platform)

