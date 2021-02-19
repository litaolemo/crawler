# -*- coding: utf-8 -*-
"""
Created on Wed May 16 12:00:37 2018

@author: hanye
"""

"""
Due to the data-type used in redis is set so I changed the word from list to set
"""



platform_redis_set_reg = {
    'toutiao': 'toutiao_url_set',
    '腾讯视频': 'v_qq_url_set',
    'youku': 'youku_url_set',
    'iqiyi': 'iqiyi_url_set',
    }


def get_redis_list_name(platform, batch_str=None):
    if platform in platform_redis_set_reg:
        platform_redis_set_name = platform_redis_set_reg[platform]
    else:
        platform_redis_set_name = None
    if batch_str is not None:
        platform_redis_set_name += '_%s' % batch_str
    return platform_redis_set_name
