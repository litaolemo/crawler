# -*- coding: utf-8 -*-
"""
Created on Tue Jun 26 14:25:37 2018

@author: hanye
"""

import argparse
import time
import datetime
from multiprocessing import Pool
from crawler_sys.framework.platform_crawler_register import get_crawler
from crawler_sys.utils.parse_bool_for_args import parse_bool_for_args

parser = argparse.ArgumentParser(description='Specify a platform name.')
parser.add_argument('-p', '--platform', default=[], action='append',
                    help=('Pass platform names, they will be assembled in python list.'))
parser.add_argument('-n', '--max_page', default=30, type=int,
                    help=('The max page numbers to be scroll for each releaser url, '
                          'must be an int value, default to 30.'))
parser.add_argument('-c', '--channels', default=[], action='append',
                    help=('Specify channel names, illegal channel names will be ignored, '
                          'default to be all.'))
parser.add_argument('-f', '--output_file_path', default=None, type=str,
                    help=('Specify output file path, default None.'))
parser.add_argument('-r', '--push_to_redis', default='False', type=str,
                    help=('Write urls to redis or not, default to True'))
parser.add_argument('-w', '--output_to_es_raw', default='True', type=str,
                    help=('Write data into es or not, default to True'))
parser.add_argument('-g', '--output_to_es_register', default='True', type=str,
                    help=('Write data into es or not, default to True'))
parser.add_argument('-s', '--processes_num', default=20, type=int,
                    help=('Processes number to be used in multiprocessing'))
args = parser.parse_args()

if args.platform != []:
    platforms = args.platform
else:
    platforms = [
        'iqiyi',
        'youku',
        '腾讯视频',
        ]
max_page = args.max_page
channel_Lst = args.channels
output_f_path = args.output_file_path
if output_f_path is None:
    output_to_file = False
else:
    output_to_file = True

push_to_redis = parse_bool_for_args(args.push_to_redis)
output_to_es_raw = parse_bool_for_args(args.output_to_es_raw)
output_to_es_register = parse_bool_for_args(args.output_to_es_register)

processes_num = args.processes_num
# multi-processing
workers = Pool(processes=processes_num)

# program exits if any of logical arguments is not parsed correctly
if (push_to_redis is None
        or output_to_es_raw is None
        or output_to_es_register is None):
    print('Program exits. %s' % datetime.datetime.now())
else:
    for platform in platforms:
        print('Platform %s' % platform)
        # get crawler for this platform
        Platform_crawler = get_crawler(platform)
        if Platform_crawler != None:
            crawler_instant = Platform_crawler()
        else:
            print('Failed to get crawler for platform %s' % platform)
            continue
        if channel_Lst == []:
            channel_Lst = crawler_instant.legal_channels
        else:
            pass

        for ch in channel_Lst:
            if ch in crawler_instant.legal_channels:
                print('platform: %s, channel: %s' % (platform, ch))
                ch_url = crawler_instant.list_page_url_dict[ch]
                args = (ch_url, ch)
                kwargs_dict = {
                    'page_num_max': max_page,
                    'output_to_file': output_to_file,
                    'filepath': output_f_path,
                    'output_to_es_raw': output_to_es_raw,
                    'output_to_es_register': output_to_es_register,
                    'push_to_redis': push_to_redis
                }
                async_res = workers.apply_async(
                    crawler_instant.list_page, args=args, kwds=kwargs_dict)
    while not async_res.ready():
        print('*** scrap_list_pages_multi_process wait for workers complete, %s'
              % datetime.datetime.now())
        time.sleep(60)

    workers.close()
    workers.join()
    print('scrap_list_pages_multi_process multiprocessing done, %s'
          % datetime.datetime.now())
