# -*- coding: utf-8 -*-
"""
Created on Thu Sep  6 09:22:24 2018

@author: fangyucheng
"""

import time
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler
from crawler.crawler_sys.utils import trans_format

def get_target_releaser_video_info(file_name,
                                   output_to_es_raw=True,
                                   es_index=None,
                                   doc_type=None,
                                   releaser_page_num_max=10000):
    start_time = int(time.time()*1e3)
    task_lst = trans_format.csv_to_lst_with_headline(file_name)
    for line in task_lst:
        releaserUrl = line['releaserUrl']
        platform = line['platform']
        crawler = get_crawler(platform=platform)
        crawler_initialization = crawler()
        if platform == 'haokan':
            try:
                crawler_initialization.releaser_page(releaserUrl=releaserUrl,
                                                     releaser_page_num_max=releaser_page_num_max,
                                                     output_to_es_raw=True,
                                                     es_index=es_index,
                                                     doc_type=doc_type,
                                                     fetchFavoriteCommnt=False)
            except:
                print(releaserUrl)
        else:
            try:
                crawler_initialization.releaser_page(releaserUrl=releaserUrl,
                                                     releaser_page_num_max=releaser_page_num_max,
                                                     output_to_es_raw=True,
                                                     es_index=es_index,
                                                     doc_type=doc_type)
            except:
                print(releaserUrl)
    end_time = int(time.time()*1e3)
    time_info = [start_time, end_time]
    return time_info

if __name__ =='__main__':
    get_time = get_target_releaser_video_info(file_name=r'/home/zhouyujiang/cuowu3.csv',
                                              releaser_page_num_max=1000,
                                              es_index='crawler-data-raw',
                                              doc_type='doc'
                                              )