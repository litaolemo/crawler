# -*- coding: utf-8 -*-
"""
根据 url 抓取 页面的播放量等信息

@author: zhouyujiang
"""

import time
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler
from crawler.crawler_sys.utils import trans_format



def get_target_video_info(platform, url):
    crawler = get_crawler(platform=platform)
    crawler_initialization = crawler()
    new_playcount = crawler_initialization.check_play_count_by_video_page(url=url)
    return new_playcount

            




#if __name__ =='__main__':
#    get_time = get_target_releaser_video_info(file_name=r'/home/zhouyujiang/cuowu3.csv',
#                                              releaser_page_num_max=1000,
#                                              es_index='crawler-data-raw',
#                                              doc_type='doc'
#                                              )