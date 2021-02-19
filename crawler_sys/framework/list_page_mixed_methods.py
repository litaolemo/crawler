# -*- coding: utf-8 -*-
"""
Created on Mon Nov 12 19:03:43 2018

@author: fangyucheng
"""

#import os
import sys
import argparse
import configparser
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler
from crawler.crawler_sys.framework.platform_crawler_register import platform_crawler_reg
from crawler.crawler_sys.utils.parse_bool_for_args import parse_bool_for_args

parser = argparse.ArgumentParser(description="crawler for video platform list page")
parser.add_argument('-p', '--platform', default=[], type=str, action='append',
                    help=('legal platform name is required'))
parser.add_argument('-c', '--conf', default=('/home/hanye/crawlersNew/crawler'
                                             '/crawler_sys/framework/config'
                                             '/list_page_urls.ini'),
                    type=str, help=('absolute path'))
parser.add_argument('-ch', '--channel', default=[], action='append', type=str,
                    help=('input all of the channel you want to scrap,'
                          'while no input means all channels'))
parser.add_argument('-fp', '--file_path', default='', type=str,
                    help=('Output data to file, default to None'))
parser.add_argument('-r', '--push_to_redis', default='False', type=str,
                    help=('Write urls to redis or not, default to True'))
parser.add_argument('-w', '--output_to_es_raw', default='True', type=str,
                    help=('Write data into es or not, default to True'))
parser.add_argument('-g', '--output_to_es_register', default='True', type=str,
                    help=('Write data into es or not, default to True'))
args = parser.parse_args()

PLATFORM_LIST = []
if args.platform != []:
    PLATFORM_LIST = args.platform
    for platform in PLATFORM_LIST:
        if platform not in platform_crawler_reg:
            print("%s is not a legal platform name,"
                  "program will exit" % platform)
            sys.exit(0)
else:
    for key, value in platform_crawler_reg.items():
        PLATFORM_LIST.append(key)
    PLATFORM_LIST.remove('haokan')
    PLATFORM_LIST.remove('腾讯新闻')
    PLATFORM_LIST.remove('miaopai')

if args.channel != []:
    CHANNEL_LIST = args.channel
else:
    CHANNEL_LIST = []

config = configparser.RawConfigParser()
config.sections()
config.read(filenames=args.conf, encoding='utf-8')

TASK_DICT = {}
for platform in PLATFORM_LIST:
    if CHANNEL_LIST == []:
        TASK_DICT[platform] = [value for key, value in config[platform].items()]
    else:
        LIST_URL_LIST = []
        for channel in CHANNEL_LIST:
            try:
                LIST_URL_LIST.append(config[platform][channel])
            except:
                print("There is no channel named %s in platform %s"
                      % (channel, platform))
        if LIST_URL_LIST == []:
            TASK_DICT[platform] = LIST_URL_LIST

FILE_PATH = args.file_path
if FILE_PATH == '':
    FILE_PATH = None
    OUTPUT_TO_FILE = False
else:
    OUTPUT_TO_FILE = True


PUSH_TO_REDIS = parse_bool_for_args(args.push_to_redis)
OUTPUT_TO_ES_RAW = parse_bool_for_args(args.output_to_es_raw)
OUTPUT_TO_ES_REGISTER = parse_bool_for_args(args.output_to_es_register)

if OUTPUT_TO_ES_RAW is True:
    ES_INDEX = 'crawler-data-raw'
    DOC_TYPE = 'doc'

#KWARGS_DICT = {'output_to_file': OUTPUT_TO_FILE,
#               'filepath': FILE_PATH,
#               'push_to_redis': PUSH_TO_REDIS,
#               'output_to_es_raw': args.output_to_es_raw,
#               'es_index': ES_INDEX,
#               'doc_type': DOC_TYPE,
#               'output_to_es_register': args.output_to_es_register}

for platform in PLATFORM_LIST:
    initialize_crawler = get_crawler(platform)
    crawler = initialize_crawler()
    TASK_LIST = TASK_DICT[platform]
    print('processing %s list page' % platform)
    crawler.list_page(task_list=TASK_LIST,
                      output_to_file=OUTPUT_TO_FILE,
                      filepath=FILE_PATH,
                      push_to_redis=PUSH_TO_REDIS,
                      output_to_es_raw=OUTPUT_TO_ES_RAW,
                      es_index=ES_INDEX,
                      doc_type=DOC_TYPE,
                      output_to_es_register=OUTPUT_TO_ES_REGISTER)
    