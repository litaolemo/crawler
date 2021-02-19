# -*- coding: utf-8 -*-
"""
Created on Thu Jun 28 15:27:39 2018

@author: hanye
"""

import argparse
import datetime
from crawler_sys.framework.platform_redis_register import get_redis_list_name
from crawler_sys.framework.redis_interact import rds
from crawler_sys.framework.platform_crawler_register import get_crawler
from crawler_sys.utils.output_results import output_result

parser = argparse.ArgumentParser(description='Specify a platform name.')
parser.add_argument('-p', '--platforms', default=[], action='append',
                    help=('Pass platform names, they will be assembled in python list.'))
parser.add_argument('-b', '--batch_str_Lst', default=[], action='append',
                    help=('Pass batch_str names, they will be assembled in python list.'))
args = parser.parse_args()

platform_Lst = args.platforms
batch_str_Lst = args.batch_str_Lst

def get_video_dict_by_url(platform, url):
    Platform_crawler = get_crawler(platform)
    if Platform_crawler != None:
        crawler_instant = Platform_crawler()
    else:
        print('Failed to get crawler for platform %s' % platform)
        return None
    video_dict = crawler_instant.video_page(url)
    return video_dict

def scrap_redis_urls(platform, batch_str):
    redis_list_name = get_redis_list_name(platform, batch_str)
    video_dict_Lst = []
    if redis_list_name is None:
        print('Failed to get correct redis list name '
              'in platform_redis_register for platform: '
              % platform)
        return None
    else:
        urls_total = rds.scard(redis_list_name)
        if urls_total == 0:
            print('Got %d urls to be processed for %s, program exits, %s'
                  % (urls_total, redis_list_name, datetime.datetime.now()))
            return None
        print('Got %d urls to be processed for %s, %s'
              % (urls_total, redis_list_name,
                 datetime.datetime.now()))
        url_bin = rds.spop(redis_list_name)
        url_counter = 1
        while url_bin is not None:
            url = url_bin.decode('utf-8')
            video_dict = get_video_dict_by_url(platform, url)
            if video_dict is not None:
                video_dict_Lst.append(video_dict)
            url_bin = rds.spop(redis_list_name)
            url_counter += 1
            if url_counter%100 == 0 or url_counter == urls_total:
                print('%s: %d/%d, %s' % (redis_list_name,
                                         url_counter,
                                         urls_total,
                                         datetime.datetime.now()))
            if len(video_dict_Lst) >= 100:
                output_result(video_dict_Lst, platform,
                              output_to_es_raw=True,
                              output_to_es_register=False,
                              push_to_redis=False,
                              output_to_file=False)
                video_dict_Lst.clear()
        if video_dict_Lst != []:
            output_result(video_dict_Lst, platform,
                          output_to_es_raw=True,
                          output_to_es_register=False,
                          push_to_redis=False,
                          output_to_file=False)
            video_dict_Lst.clear()

if platform_Lst == []:
    print('No platform is given, program exits.')
else:
    for platform in platform_Lst:
        print('Scraping platform: %s' % platform)
        if batch_str_Lst == []:
            batch_str = ''
            scrap_redis_urls(platform, batch_str)
        else:
            for batch_str in batch_str_Lst:
                print('platform: %s batch: %s' % (platform, batch_str))
                scrap_redis_urls(platform, batch_str)
