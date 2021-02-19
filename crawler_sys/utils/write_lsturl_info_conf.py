# -*- coding: utf-8 -*-
"""
Created on Wed Oct 10 17:06:49 2018

@author: fangyucheng
"""

import os
import configparser

conf_file_path = os.getcwd()
tencent_dic = {"platform": "腾讯视频",
               "channel": {"音乐": "http://v.qq.com/x/list/music",
                           "新闻": "http://v.qq.com/x/list/news",
                           "军事": "http://v.qq.com/x/list/military",
                           "娱乐": "http://v.qq.com/x/list/ent",
                           "体育": "http://v.qq.com/x/list/sports",
                           "游戏": "http://v.qq.com/x/list/games",
                           "搞笑": "http://v.qq.com/x/list/fun",
                           "时尚": "http://v.qq.com/x/list/fashion",
                           "生活": "http://v.qq.com/x/list/life",
                           "母婴": "http://v.qq.com/x/list/baby",
                           "汽车": "http://v.qq.com/x/list/auto",
                           "科技": "http://v.qq.com/x/list/tech",
                           "教育": "http://v.qq.com/x/list/education",
                           "财经": "http://v.qq.com/x/list/finance",
                           "房产": "http://v.qq.com/x/list/house",
                           "旅游": "http://v.qq.com/x/list/travel",
                           "王者荣耀": "http://v.qq.com/x/list/kings"}}

target_lst_page_lst = [tencent_dic,]

#initialize conf file
config = configparser.ConfigParser()
for platform_dic in target_lst_page_lst:
    config[platform_dic['platform']] = platform_dic['channel']
with open(conf_file_path + '/lst_page_conf.ini',
          'w', encoding='utf-8') as configfile:
    config.write(configfile)
    