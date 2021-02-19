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

import argparse
from crawler_sys.framework.platform_crawler_register import get_crawler
from crawler_sys.framework.es_target_releasers import get_releaserUrls_from_es
from crawler_sys.utils.parse_bool_for_args import parse_bool_for_args

parser = argparse.ArgumentParser(description='Specify a platform name.')
parser.add_argument('-p', '--platform', default=[], action='append',
                    help=('Pass platform names, they will be assembled in python list.'))
parser.add_argument('-n', '--max_page', default=30, type=int,
                    help=('The max page numbers to be scroll for each releaser url, '
                          'must be an int value, default to 30.'))
parser.add_argument('-f', '--output_file_path', default='', type=str,
                    help=('Specify output file path, default None.'))
parser.add_argument('-r', '--push_to_redis', default='False', type=str,
                    help=('Write urls to redis or not, default to True'))
parser.add_argument('-w', '--output_to_es_raw', default='True', type=str,
                    help=('Write data into es or not, default to True'))
parser.add_argument('-g', '--output_to_es_register', default='True', type=str,
                    help=('Write data into es or not, default to True'))
parser.add_argument('-l', '--releasers', default=[], action='append',
                    help=('Write data into es or not, default to True'))
parser.add_argument('-t', '--target_index', default="target_releasers", type=str,
                    help=('target_releasers_org or target_releasers'))
args = parser.parse_args()

if args.platform != []:
    platforms = args.platform
else:
    platforms = [
        'toutiao',
        '腾讯视频',
        'iqiyi',
        'youku',
        '腾讯新闻',
        'haodkan',
        'new_tudou',
            "kwai"
        ]
releaser_page_num_max = args.max_page
output_f_path = args.output_file_path
if output_f_path == '':
    output_to_file = False
else:
    output_to_file = True

push_to_redis = parse_bool_for_args(args.push_to_redis)
output_to_es_raw = parse_bool_for_args(args.output_to_es_raw)
output_to_es_register = parse_bool_for_args(args.output_to_es_register)

releaser_Lst = args.releasers

for platform in platforms:
    # 2 get releaserUrl list on each platform from target-releasers index
    if not releaser_Lst:
        releaserUrl_Lst = get_releaserUrls_from_es(platform=platform,target_index=args.target_index)
    else:
        releaserUrl_Lst = []
        for releaser in releaser_Lst:
            releaserUrl_Lst.extend(get_releaserUrls_from_es(platform=platform,target_index=args.target_index))
    if releaserUrl_Lst == []:
        print('Get empty releaserUrl_Lst for platform %s' % platform)
        continue
    # 3 get crawler for this platform
    Platform_crawler = get_crawler(platform)
    if Platform_crawler != None:
        crawler_instant = Platform_crawler()
    else:
        print('Failed to get crawler for platform %s' % platform)
        continue
    # 4 for each releaserUrl, get data on the releaser page identified by this
    # releaser url
    for releaserUrl in releaserUrl_Lst:
        crawler_instant.releaser_page(releaserUrl[0],
                                      output_to_file=output_to_file,
                                      filepath=output_f_path,
                                      releaser_page_num_max=releaser_page_num_max,
                                      output_to_es_raw=output_to_es_raw,
                                      output_to_es_register=output_to_es_register,
                                      push_to_redis=push_to_redis
                                     )
