# -*- coding:utf-8 -*-
# @Time : 2019/7/19 17:09 
# @Author : litao
# -*- coding: utf-8 -*-

import argparse
from elasticsearch.helpers import scan
from elasticsearch import Elasticsearch
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler
from multiprocessing import Pool

PARSER = argparse.ArgumentParser(description='video platform search page crawler')
# PARSER.add_argument('-c', '--conf', default=('/home/hanye/crawlersNew/crawler'
#                                              '/crawler_sys/framework/config'
#                                              '/search_keywords.ini'),
#                    help=('config file absolute path'))
PARSER.add_argument('-p', '--platform', default=["toutiao","腾讯新闻", "腾讯视频", "new_tudou"], action='append',
                    help=('legal platform name is required'))
PARSER.add_argument('-k', '--key_word_platform', default=[], action='append',
                    help=('key_word_legal platform name is required'))
PARSER.add_argument('-w', '--output_to_es_raw', default=True,
                    help=('output to es raw'))
PARSER.add_argument('-g', '--output_to_es_register', default=False,
                    help=('output to es register'))
PARSER.add_argument('-n', '--maxpage', default=20,
                    help=('maxpage'))

ARGS = PARSER.parse_args()
es_framework = Elasticsearch(hosts='192.168.17.11', port=80,
                             http_auth=('crawler', 'XBcasfo8dgfs'))

index_target_releaser = 'search_keywords'
doc_type_target_releaser = 'doc'

# index_target_releaser = 'test2'
# doc_type_target_releaser = 'keywrod'


if ARGS.platform != []:
    PLATFORM_LIST = ARGS.platform
# for platform in PLATFORM_LIST:
#     if platform not in legal_platform_name:
#         print("%s is not a legal platform name, "
#               "program will exit" % platform)
#         sys.exit(0)

# CONFIG = configparser.ConfigParser()
# CONFIG.read(ARGS.conf, encoding='utf-8')

OUTPUT_TO_ES_RAW = ARGS.output_to_es_raw
OUTPUT_TO_ES_REGISTER = ARGS.output_to_es_register


def func_search_keywordlist(platform):
    search_body = {"query": {"bool": {"filter": []}}}
    search_resp = es_framework.search(index=index_target_releaser,
                                      doc_type=doc_type_target_releaser,
                                      body=search_body,
                                      size=0,
                                      request_timeout=100)
    total_hit = search_resp['hits']['total']
    releaser_dic = {}
    if total_hit > 0:
        print('Got %d releaser for platform %s.' % (total_hit, platform))
        scan_resp = scan(client=es_framework, query=search_body,
                         index=index_target_releaser,
                         doc_type=doc_type_target_releaser,
                         request_timeout=200)
        for line in scan_resp:
            try:
                title = line['_source']['title']
                page = line['_source']['page']
                releaser_dic[title] = page
            except:
                print('error in :', line)
                continue
    else:
        print('Got zero hits.')
    return releaser_dic


if OUTPUT_TO_ES_RAW is True:
    ES_INDEX = 'crawler-data-raw'
    # ES_INDEX = 'test2'
    DOC_TYPE = 'doc'
    print(ES_INDEX, DOC_TYPE)
pages = ARGS.maxpage


def search_page_task(platform, output_to_es_raw,
                     output_to_es_register,
                     es_index,
                     doc_type):
    search_pages = []
    initialize_crawler = get_crawler(platform)
    crawler = initialize_crawler()
    KEYWORD_dic = func_search_keywordlist(platform)
    for keyword in KEYWORD_dic:
        print("search keyword '%s' on platform %s" % (keyword, platform))
        search_pages = int(KEYWORD_dic[keyword])
        try:
            if platform != "腾讯新闻":
                crawler.search_page(keyword=keyword,
                                    search_pages_max=search_pages,
                                    output_to_es_raw=output_to_es_raw,
                                    output_to_es_register=output_to_es_register,
                                    es_index=es_index,
                                    doc_type=doc_type)
            else:
                crawler.search_video_page(keyword, None,
                                          search_pages_max=search_pages,
                                          output_to_es_raw=output_to_es_raw,
                                          output_to_es_register=output_to_es_register,
                                          es_index=es_index,
                                          doc_type=doc_type,releaser=False)
        except Exception as e:
            print(e)
            continue


result = []
kwargs_dict = {
        'output_to_es_raw': OUTPUT_TO_ES_RAW,
        'output_to_es_register': OUTPUT_TO_ES_REGISTER,
        'es_index': ES_INDEX,
        'doc_type': DOC_TYPE,

}
pool = Pool(processes=4)
for platform in PLATFORM_LIST:
    res = pool.apply_async(func=search_page_task, args=(platform,OUTPUT_TO_ES_RAW,OUTPUT_TO_ES_REGISTER,ES_INDEX,DOC_TYPE))
    result.append(res)
pool.close()
pool.join()

print('=================')
for i in result:
    print(i.get())

# config file absolute path in serve
# '/home/hanye/crawlers/crawler_sys/framework/config/search_keywords.ini'
