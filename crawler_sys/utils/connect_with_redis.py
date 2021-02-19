# -*- coding: utf-8 -*-
"""
Created on Sun Nov 11 00:38:51 2018

@author: fangyucheng
"""

import json
#import time
import redis

rds = redis.StrictRedis(host='192.168.17.60', port=6379, db=13)

#push and retrieve list url into redis
def push_list_url_to_redis(platform, result_lst):
    """push a list of url(only url, type str) into a redis list
    """
    if platform == '腾讯视频':
        platform = 'v_qq'
    key = "%s_list_url" % platform
    for line in result_lst:
        rds.lpush(key, line)
    print("the length of %s is %s" % (key, rds.llen(key)))

def retrieve_list_url_from_redis(platform, retrieve_count=30):
    if platform == '腾讯视频':
        platform = 'v_qq'
    key = '%s_list_url' % platform
    count = 0
    result_lst = []
    while retrieve_count > count:
        url_bytes = rds.rpop(key)
        if url_bytes is None:
            print("retrieve %s list urls from redis" % len(result_lst))
            return result_lst
        else:
            url = url_bytes.decode("utf-8").replace("\'", "\"")
            result_lst.append(url)
            count += 1
    print("retrieve %s list urls from redis" % len(result_lst))
    return result_lst
#end of pushing and retrieving list url into redis

#push and retrieve list page html
def push_list_page_html_to_redis(platform, result_lst):
    """
    push download list page html to redis,
    it only used for v_qq now
    """
    if platform == '腾讯视频':
        platform = 'v_qq'
    key = '%s_list_page_html' % platform
    for line in result_lst:
        rds.lpush(key, line)
    print("the length of lst_page_html is %s" % rds.llen(key))

def retrieve_list_page_html_from_redis(platform):
    if platform == '腾讯视频':
        platform = 'v_qq'
    key = '%s_list_page_html' % platform
    lst_page_html_byte = rds.rpop(key)
    lst_page_html_str = lst_page_html_byte.decode("utf-8").replace("\'", "\"")
    return lst_page_html_str
#end of pushing and retrieving list page html


#this is to push url into redis set only url
def push_video_url_to_redis_set(platform, url_lst):
    """
    push url to redis set
    it usually be used in renewing video page play_count 
    and special platform list page crawler such as new_tudou
    """
    if platform == '腾讯视频':
        platform = 'v_qq'
    key = '%s_video_url' % platform
    for line in url_lst:
        rds.sadd(key, line)
    print("the length of %s set is %s" % (key, rds.scard(key)))

def retrieve_video_url_from_redis_set(platform, retrieve_count=90):
    """
    retrieve video url from redis set
    """
    if platform == '腾讯视频':
        platform = 'v_qq'
    key = '%s_video_url' % platform
    url_list = []
    count = 0
    while retrieve_count > count:
        url_byte = rds.spop(key)
        if url_byte is None:
            print("total output platform %s %s urls"
                  % (platform, len(url_list)))
            return url_list
        else:
            url_str = url_byte.decode('utf-8').replace("\'", "\"")
            url_list.append(url_str)
            count += 1
    print("total output platform %s %s urls" % (platform, len(url_list)))
    return url_list
#end of above part

#push and retireve url dict to redis
def push_url_dict_lst_to_redis_set(platform, result_lst):
    """
    push a dict with url to redis,
    generally, the dict is from list page
    """
    if platform == '腾讯视频':
        platform = 'v_qq'
    key = '%s_url_dict' % platform
    for line in result_lst:
        rds.sadd(key, line)
    print("the length of %s urldict set is %s" % (key, rds.scard(key)))

def retrieve_url_dict_from_redis_set(platform, retrieve_count=90):
    """
    retrieve a dict with url from redis
    """
    if platform == '腾讯视频':
        platform = 'v_qq'
    key = '%s_url_dict' % platform
    count = 0
    result_lst = []
    while retrieve_count > count:
        url_dic_bytes = rds.spop(key)
        if url_dic_bytes is None:
            print("total output %s url dicts" % len(result_lst))
            return result_lst
        else:
            url_dic_str = url_dic_bytes.decode('utf-8').replace("\'", "\"")
            url_dic = json.loads(url_dic_str)
            count += 1
            result_lst.append(url_dic)
    print("total output %s from %s urldicts" % (len(result_lst), platform))
    return result_lst
#end of push and retireve url dict to redis

#push and retrieve video page html
def push_video_page_html_to_redis(platform, result_lst):
    """
    push download video page html to redis
    """
    if platform == '腾讯视频':
        platform = 'v_qq'
    key = '%s_video_page_html' % platform
    for line in result_lst:
        rds.lpush(key, line)
    print("the length of %s list is %s" % (key, rds.llen(key)))

def retrieve_video_page_html_from_redis(platform):
    """
    retrieve html both video page and list page from redis
    """
    if platform == '腾讯视频':
        platform = 'v_qq'
    key = '%s_video_page_html' % platform
    video_html_bytes = rds.rpop(key)
    video_html_str = video_html_bytes.decode("utf-8")
    return video_html_str
#end of push and retrieve video page html

def length_of_lst(key):
    """
    To get the length of a redis list
    """
    length = rds.llen(key)
    return length


def push_error_html_to_redis(error_page):
    """
    Used for asynchronous crawler, to push error list page into redis
    """
    rds.lpush('error', error_page)



"""
this part is for renew video data
"""
#set for url
platform_redis_set_reg = {'toutiao': 'toutiao_url_set',
                          '腾讯视频': 'v_qq_url_set',
                          'youku': 'youku_url_set',
                          'iqiyi': 'iqiyi_url_set',
                          }
#list for html
platform_redis_lst_reg = {'toutiao': 'toutiao_html_lst',
                          '腾讯视频': 'v_qq_html_lst',
                          'youku': 'youku_html_lst',
                          'iqiyi': 'iqiyi_html_lst'}


#def retrieve_video_url_from_redis_set(platform, retrieve_count=90):
#    count = 0
#    result_lst = []
#    redis_key = platform_redis_set_reg[platform]
#    while retrieve_count > count:
#        url_bytes = rds.spop(redis_key)
#        if url_bytes is None:
#            print("total output %s urls" % len(result_lst))
#            return result_lst
#        else:
#            url_str = url_bytes.decode('utf-8').replace("\'", "\"")
#            count += 1
#            result_lst.append(url_str)
#    print("total output %s urls" % len(result_lst))
#    return result_lst

def push_video_page_html_to_redis_renew(platform, result_lst):
    redis_key = platform_redis_lst_reg[platform]
    for line in result_lst:
        rds.lpush(redis_key, line)
    print("the length of %s is %s" % (redis_key, rds.llen(redis_key)))

def retrieve_video_html_from_redis_renew(platform):
    redis_key = platform_redis_lst_reg[platform]
    video_html_bytes = rds.rpop(redis_key)
    video_html_str = video_html_bytes.decode("utf-8").replace("\'", "\"")
    return video_html_str

def length_of_set(key):
    length = rds.scard(key)
    return length

#this is for iqiyi
def iqiyi_push(video_dict):
    key = 'iqiyi_video_info' 
    rds.lpush(key, video_dict)
    print("the length of iqiyi list is %s" % rds.llen(key))

def iqiyi_retrieve(retrieve_count=90):
    key = 'iqiyi_video_info'
    info_list = []
    count = 0
    while retrieve_count > count:
        info_dict_byte = rds.rpop(key)
        if info_dict_byte is None:
            print("total output %s urls" % len(info_list))
            return info_list
        else:
            info_str = info_dict_byte.decode('utf-8').replace("\'", "\"")
            info_dict = json.loads(info_str)
            info_list.append(info_dict)
            count += 1
    print("total output %s urls" % len(info_list))
    return info_list

#this is a test purpose
def push_pid_to_redis_set(platform, step, value):
    """step is to determine which step the crawler is working on,
    such as parse_list_page, download_video_page, parse_video_page
    """
    if platform == '腾讯视频':
        platform = 'v_qq'
    key = '%s_%s_pid' % (platform, step)
    rds.sadd(key, value)
    print('push %s to redis set %s' % (value, key))

def delete_pid_from_redis_set(platform, step, value):
    """step is to determine which step the crawler is working on,
    such as parse_list_page, download_video_page, parse_video_page
    """
    if platform == '腾讯视频':
        platform = 'v_qq'
    key = '%s_%s_pid' % (platform, step)
    rds.srem(key, value)
    print('delete %s from redis set %s' % (value, key))
