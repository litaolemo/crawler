# -*- coding: utf-8 -*-
"""
Created on Tue Jun 26 14:25:37 2018

@author: hanye
"""

import argparse
import configparser
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler
from crawler.crawler_sys.framework.platform_crawler_register import platform_crawler_reg

parser = argparse.ArgumentParser(description='Specify a platform name.')
parser.add_argument('-p', '--platform', default=[], action='append',
                    help=('Pass platform names, they will be assembled in python list.'))
parser.add_argument('-c', '--conf', default=('/home/hanye/crawlersNew/crawler'
                                             '/crawler_sys/framework/config'
                                             '/list_page_urls.ini'), type=str,
                    help=('absolute path of config file'))
parser.add_argument('-ch', '--channels', default=[], action='append',
                    help=('Specify channel names, illegal channel names will be ignored, '
                          'default to be all.'))
args = parser.parse_args()

if args.platform != []:
    platforms = args.platform
    for platform in platforms:
        if platform not in platform_crawler_reg:
            print("%s is not a legal platform name" % platform)
else:
    platforms = [
        'iqiyi',
        'youku',
        '腾讯视频',
        'new_tudou',
        'toutiao'
        ]
config_file = args.conf
config = configparser.RawConfigParser()
config.sections()
config.read(config_file)
channel_Lst = args.channels

for platform in platforms:
    print('working on Platform %s' % platform)
    Platform_crawler = get_crawler(platform)
    crawler = Platform_crawler()
    task_list = []
    if channel_Lst == []:
        for key, value in config[platform].items():
            task_list.append(value)
    else:
        for channel in channel_Lst:
            try:
                task_url = config[platform][channel]
                task_list.append(task_url)
            except:
                print("there is no channel %s in platform %s" % (channel, platform))
    crawler.start_list_page(task_list)
    
