# -*- coding: utf-8 -*-
"""
Created on Fri Dec 14 10:32:28 2018

@author: fangyucheng
"""

import time
from multiprocessing import Process
from crawler.crawler_sys.utils import connect_with_redis
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler


"""
only for platform v_qq, iqiyi and youku
"""

step = 'parse_list_page'
data_cate = 'list_page_html'

def parse_list_page(platform):
    crawler_initialization = get_crawler(platform)
    crawler = crawler_initialization()
    if platform == '腾讯视频':
        platform = 'v_qq'
    key_lst = "%s_%s" % (platform, data_cate)
#    key_set = "%s_%s" % (platform, step)
    while True:
        if connect_with_redis.length_of_lst(key=key_lst) > 0:
            crawler.parse_list_page_multi_process()
        else:
            print("no %s list page html in redis" % platform)
            time.sleep(300)

v_qq = Process(target=parse_list_page, args=('腾讯视频',))
iqiyi = Process(target=parse_list_page, args=('iqiyi',))
youku = Process(target=parse_list_page, args=('youku',))

v_qq.start()
iqiyi.start()
youku.start()
