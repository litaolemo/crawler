# -*- coding: utf-8 -*-
"""
Created on Tue Jun  5 18:53:49 2018

@author: hanye
"""

#import redis
from crawler_sys.framework.platform_crawler_register import get_crawler
from crawler_sys.framework.platform_redis_register import get_redis_list_name
from crawler_sys.utils.output_results import output_result
import time
from crawler_sys.framework.redis_interact import rds

#rds=redis.StrictRedis(host='192.168.17.26',port=6379,db=0)
seconds_to_sleep_between_waitings_for_redis_list = 60

def crawle_platform(platform,
                    write_into_file=False,
                    will_write_into_es=True):
    Platform_crawler = get_crawler(platform)
    if Platform_crawler == None:
        print('Failed to get crawler for platform %s' % platform)
    else:
        crawler_instant = Platform_crawler()
        redis_list = get_redis_list_name(platform)
        video_Lst = []
        crawler_counter = 0
        if redis_list!=None:
            while True:
                url = rds.rpop(redis_list).decode()
                if url!=None: # which means get url from redis sucessfully
                    video_dict = crawler_instant.video_page(url)
                    if video_dict!=None:
                        video_Lst.append(video_dict)
                        crawler_counter += 1
                else:
                    print('Empty redis list, wait...')
                    time.sleep(seconds_to_sleep_between_waitings_for_redis_list)
                if crawler_counter%1000==0:
                    print('crawle_server: writing 1000 lines into es, '
                          'platform %s crawler_couter: %d'
                          %(platform, crawler_counter))
                    output_result(video_Lst, platform,
                                  output_to_es=will_write_into_es)
                    video_Lst.clear()
            if video_Lst!=[]:
                output_result(video_Lst, platform,
                              output_to_es=will_write_into_es)
                video_Lst.clear()

