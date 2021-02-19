# -*- coding: utf-8 -*-
"""
Created on Wed Jun  6 18:18:09 2018

@author: hanye
"""
#import redis
#from crawler_sys.framework.platform_redis_register import get_redis_list_name
from crawler.crawler_sys.framework.es_crawler import scan_crawler_url_register

#rds = redis.StrictRedis(host='192.168.17.60', port=6379, db=0)

def pull_url_from_es(platform, release_time_lower_bdr=None):
    """
    Just pull urls from es index crawler-url-register.
    Url reforming things will be done in the method who
    is responsible for pushing urls into redis.
    Just return url and its platform 
    """
    if release_time_lower_bdr is None:
        release_time_lower_bdr = 0
    else:
        pass
    search_body = {"query": {"bool": {"filter": [{"range": {"release_time":
                                                 {"gte": release_time_lower_bdr}}},
                                                 {"term": {"platform.keyword": platform}}]}}}
    total_hit, scan_resp = scan_crawler_url_register(search_body)
    batch_url_Lst = []
    if total_hit > 0:
        line_counter = 0
        for line in scan_resp:
            line_counter += 1
            line_d = line['_source']
            url = line_d['url']
            batch_url_Lst.append(url)
    else:
        pass
    return batch_url_Lst


#def url_reformer(platform, url):
#    """
#    to reform url according to platform, in the future.
#    Say, a url of http://www.toutiao.com/group/1234567890123456789
#    as a string is different from http://www.365yg.com/u/1234567890123456789,
#    but they point to the same resource. They should be reformed
#    to one unique url before pushing into redis for futher crawling.
#    """
#    reformed_url = url
#    return reformed_url
#
#def feed_url_into_redis(dict_Lst, platform,
#                        release_time_lower_bdr=None,
#                        batch_str=None):
#    """
#    release_time_lower_bdr must be an int value represent
#    timestamp in milliseconds if given.
#    All url that is released before release_time_lower_bdr
#    will not be pushed into redis. If argument release_time_lower_bdr
#    is not given when call this function, all urls will be
#    pushed into redis.
#    """
#    redis_list_name = get_redis_list_name(platform, batch_str)
#    if redis_list_name is None:
#        print('Failed to get correct redis list name '
#              'in platform_redis_register for platform: '
#              % platform)
#        return (None, None)
#    else:
#        print('Feeding url into redis list %s ...' % redis_list_name)
#        url_counter = 0
#        for data_dict in dict_Lst:
#            try:
#                url = data_dict['url']
#                url_reformed = url_reformer(platform, url)
#                if release_time_lower_bdr is None:
#                    sadd_c = rds.sadd(redis_list_name, url_reformed)
#                    url_counter += sadd_c
#                else:
#                    url_release_time = data_dict['release_time']
#                    if url_release_time >= release_time_lower_bdr:
#                        sadd_c = rds.sadd(redis_list_name, url_reformed)
#                        url_counter += sadd_c
#            except:
#                print('Failed to push url into redis, '
#                      'might because of lack of url field '
#                      'or lack of release_time field, or '
#                      'has wrong typed release_time value. '
#                      'The failed data dict is: \n %s' % data_dict)
#        print('Pushed %d urls into redis' % url_counter)
#        return (redis_list_name, url_counter)

