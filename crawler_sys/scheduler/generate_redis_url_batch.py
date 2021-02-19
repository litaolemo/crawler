# -*- coding: utf-8 -*-
"""
Created on Fri Jun 29 13:09:04 2018

@author: hanye
"""

import argparse
import datetime
from crawler_sys.framework.redis_interact import pull_url_from_es
from crawler_sys.framework.redis_interact import feed_url_into_redis

parser = argparse.ArgumentParser(description='')
parser.add_argument('-p', '--platforms', default=[], action='append',
                    help=('Pass platform names, they will be assembled in python list.'))
parser.add_argument('-b', '--batch_str_Lst', default=[], action='append',
                    help=('Pass batch_str names, they will be assembled in python list.'))
parser.add_argument('-d', '--days_from_now', default=30, type=int,
                    help=('Specify days from now as the lower boundary for release_time, '
                          'default 30.'))
args = parser.parse_args()

def redis_url_batch_gen(platform, batch_str, release_time_lower_bdr):
    url_Lst = pull_url_from_es(platform, release_time_lower_bdr)
    if url_Lst != []:
        redis_list_name, push_counter = feed_url_into_redis(url_Lst, platform,
                                                            batch_str=batch_str)
        return (redis_list_name, push_counter)
    else:
        return (None, None)

platform_Lst = args.platforms
batch_str_Lst = args.batch_str_Lst
if batch_str_Lst == []:
    batch_str_Lst.append('')
days_from_now = args.days_from_now
release_time_lower_bdrD = datetime.date.today() - datetime.timedelta(days_from_now)
release_time_lower_bdr_ts = int(datetime.datetime(release_time_lower_bdrD.year,
                                                  release_time_lower_bdrD.month,
                                                  release_time_lower_bdrD.day)
                                .timestamp() * 1e3)

if platform_Lst == []:
    print('No platform specified, program exits.')
else:
    for platform in platform_Lst:
        print('Generating redis url list for platform: %s' % platform)
        for batch_str in batch_str_Lst:
            print('platform: %s batch: %s' % (platform, batch_str))
            redis_list_name, push_counter = redis_url_batch_gen(
                platform,
                batch_str=batch_str,
                release_time_lower_bdr=release_time_lower_bdr_ts
                )
