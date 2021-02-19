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
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Pool
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler
from crawler.crawler_sys.framework.es_target_releasers import get_releaserUrls_from_es
from crawler.crawler_sys.framework.platform_crawler_register import platform_crawler_reg
from crawler.crawler_sys.utils.parse_bool_for_args import parse_bool_for_args

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
parser.add_argument('-index', '--es_index', default='crawler-data-raw', type=str,
                    help=('assign a es_index to write into, default to crawler-data-raw'))
parser.add_argument('-doc', '--doc_type', default='doc', type=str,
                    help=('assign a doc to write into, default to doc'))
parser.add_argument('-g', '--output_to_es_register', default='True', type=str,
                    help=('Write data into es or not, default to True'))
parser.add_argument('-l', '--releasers', default=[], action='append',
                    help=('Write data into es or not, default to True'))
parser.add_argument('-fre', '--frequency', default=0, type=int,
                    help=('choose a frequency to retrieve releaserUrl,'
                          '1, 3 or 9 is legal number, default 0'))
parser.add_argument('-s', '--processes_num', default=30, type=int,
                    help=('Processes number to be used in multiprocessing'))
parser.add_argument('-v', '--video', default="False", type=str,
                    help=('Is or not run video_page_crawler'))
parser.add_argument('-t', '--target_index', default="target_releasers", type=str,
                    help=('target_releasers_org or target_releasers'))
parser.add_argument('-file', '--file', default="", type=str,
                    help=('target_releasers_org or target_releasers'))
args = parser.parse_args()

# if args.platform != []:
#     platforms = args.platform
# else:
#     print('platform must be input')
#     sys.exit(0)
#
# for platform in platforms:
#     if platform not in platform_crawler_reg:
#         print("illegal platform name %s" % platform)
#         sys.exit(0)
if __name__ == "__main__":
    platforms = ["腾讯新闻"]
    platform = "腾讯新闻"
    releaser_page_num_max = args.max_page
    output_f_path = args.output_file_path
    frequency = args.frequency

    if output_f_path == '':
        output_f_path = None
    if frequency == '':
        frequency = None

    if output_f_path is None:
        output_to_file = False
    else:
        output_to_file = True

    push_to_redis = parse_bool_for_args(args.push_to_redis)
    output_to_es_raw = parse_bool_for_args(args.output_to_es_raw)
    output_to_es_register = parse_bool_for_args(args.output_to_es_register)

    releasers = args.releasers
    processes_num = args.processes_num
    frequency = args.frequency
    print(frequency)

    if frequency == 0:
        frequency = None

    es_index = args.es_index
    doc_type = args.doc_type

    kwargs_dict = {
       'output_to_file': output_to_file,
       'filepath': output_f_path,
       'releaser_page_num_max': releaser_page_num_max,
       'output_to_es_raw': output_to_es_raw,
       'es_index': es_index,
       'doc_type': doc_type,
     'output_to_es_register': output_to_es_register,
     #   'push_to_redis': push_to_redis,
    }
    for platform in platforms:
        # 2 get releaserUrl list on each platform from target-releasers index
        if releasers == []:
            releaserUrl_Lst = get_releaserUrls_from_es(platform=platform, frequency=frequency,target_index=args.target_index)
        else:
            releaserUrl_Lst = []
            for releaser in releasers:
                releaserUrl_Lst.extend(get_releaserUrls_from_es(platform=platform, releaser=releaser, frequency=frequency,target_index=args.target_index))
        if releaserUrl_Lst == []:

            print('Get empty releaserUrl_Lst for platform %s' % platform)
            continue
        # 3 get crawler for this platform
        Platform_crawler = get_crawler(platform)
        # print(releaserUrl_Lst)
        if Platform_crawler != None:
            crawler_instant = Platform_crawler()
            if args.video == "True":
                try:
                    crawler = crawler_instant.video_page
                except:
                    crawler = crawler_instant.search_video_page
            else:
                crawler = crawler_instant.releaser_page

        else:
            print('Failed to get crawler for platform %s' % platform)
            continue
        # 4 for each releaserUrl, get data on the releaser page identified by this
        # releaserUrl, with multiprocesses
        # pool = Pool(processes=processes_num)
        executor = ProcessPoolExecutor(max_workers=5)
        futures = []
        if platform == "腾讯新闻" and args.video == "True":
            crawler = crawler_instant.search_video_page
            for url, releaser in releaserUrl_Lst:
                print(releaser, url)
            #     pool.apply_async(func=crawler, args=(releaser, url), kwds=kwargs_dict)
            # pool.close()
            # pool.join()
            print('Multiprocessing done for platform %s' % platform)
        else:
            for url, releaser in releaserUrl_Lst:
                # pool.apply_async(func=crawler, args=(url,), kwds=kwargs_dict)
                # print(url)
                # crawler(url,output_to_es_raw =True, es_index = 'crawler-data-raw', doc_type = 'doc', releaser_page_num_max = 50
                #  )
                future = executor.submit(crawler, url
                , output_to_es_raw=True, es_index='crawler-data-raw',doc_type='doc', releaser_page_num_max=80)
                futures.append(future)
            # pool.close()
            # pool.join()
            executor.shutdown(True)
            print('Multiprocessing done for platform %s' % platform)


