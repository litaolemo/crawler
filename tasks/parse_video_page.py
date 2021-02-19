# -*- coding: utf-8 -*-
"""
Created on Fri Dec 14 15:23:29 2018

@author: fangyucheng
"""

import time
from multiprocessing import Process
from crawler.crawler_sys.utils import connect_with_redis
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler

"""
for platform v_qq, iqiyi, and new_tudou
"""

kwarg_dict = {'output_to_es_raw': True,
              'es_index': 'crawler-data-raw',
              'doc_type': 'doc',
              'output_to_es_register': True}

step = 'parse_video_page'
data_cate = 'video_page_html'

def parse_video_page(platform, para_dic):
    crawler_initialization = get_crawler(platform)
    crawler = crawler_initialization()
    if platform == '腾讯视频':
        platform = 'v_qq'
    key= "%s_%s" % (platform, data_cate)
    while True:
        if connect_with_redis.length_of_lst(key) > 0:
            crawler.parse_video_page_multi_process(para_dic)
        else:
            print("no %s video page html in redis" % platform)
            time.sleep(300)

v_qq = Process(target=parse_video_page, args=('腾讯视频', kwarg_dict))
iqiyi = Process(target=parse_video_page, args=('iqiyi', kwarg_dict))
new_tudou = Process(target=parse_video_page, args=('new_tudou', kwarg_dict))

v_qq.start()
iqiyi.start()
new_tudou.start()