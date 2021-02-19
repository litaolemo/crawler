# -*- coding: utf-8 -*-
"""
Created on Fri Dec 14 15:03:05 2018

@author: fangyucheng
"""

import time
from multiprocessing import Process
from crawler.crawler_sys.utils import connect_with_redis
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler


"""
only for platform v_qq, iqiyi, and new_tudou
"""

step = 'download_video_page'
data_cate = 'video_url'

def download_video_page(platform):
    crawler_initialization = get_crawler(platform)
    crawler = crawler_initialization()
    if platform == '腾讯视频':
        key = 'v_qq_url_dict'
    else:
        key= "%s_%s" % (platform, data_cate)
    while True:
        if connect_with_redis.length_of_set(key) > 0:
#            pid_num = connect_with_redis.length_of_set(key=key_set)
#            if pid_num < 20:
#                process_num = int(20-pid_num)
            crawler.download_video_page_async_multi_process()
#            else:
#                print("%s processes is working on %s" % (pid_num, platform))
#                time.sleep(20)
        else:
            print("no %s url [dict] in redis" % platform)
            time.sleep(300)

v_qq = Process(target=download_video_page, args=('腾讯视频',))
iqiyi = Process(target=download_video_page, args=('iqiyi',))
new_tudou = Process(target=download_video_page, args=('new_tudou',))

v_qq.start()
iqiyi.start()
new_tudou.start()