# -*- coding: utf-8 -*-
"""
Created on Wed Nov 21 10:24:31 2018

@author: fangyucheng
"""

import argparse
from crawler.crawler_sys.utils.connect_with_redis import push_video_url_to_redis_set
from crawler.crawler_sys.utils.connect_with_es import pull_url_from_es
from crawler.crawler_sys.utils.date_calculator import calculator
 
parser = argparse.ArgumentParser(description='')
parser.add_argument('-p', '--platforms', default=[], action='append',
                    help=('Pass platform names, they will be assembled in python list.'))
parser.add_argument('-d', '--days_from_now', default=30, type=int,
                    help=('Specify days from now as the lower boundary for release_time, '
                          'default 30.'))
args = parser.parse_args()

platform_Lst = args.platforms
release_time_lower_bdr = calculator(args.days_from_now)

if platform_Lst == []:
    print("Please input at least one platform")
else:
    for platform in platform_Lst:
        download_es_register = pull_url_from_es(platform=platform,
                                                release_time_lower_bdr=release_time_lower_bdr)
        print("successfully downloaded data from es, platform: %s" % platform)
        push_to_redis = push_video_url_to_redis_set(platform=platform,
                                                    url_lst=download_es_register)
        print("successfully pushed into redis, count of url: %s" % push_to_redis)