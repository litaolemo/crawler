# -*- coding:utf-8 -*-
# @Time : 2020/5/19 17:31 
# @Author : litao
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 27 19:06:38 2018

input platform and releaserUrl, output follower number to file

@author: fangyucheng
"""

import redis , json
from crawler.crawler_sys.utils import trans_format
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler
rds3 = redis.StrictRedis(host='192.168.17.60', port=6379, db=3, decode_responses=True)

input_file = r"D:\work_file\发布者账号\一次性需求附件\体育部.csv"
tast_list = trans_format.csv_to_lst_with_headline(input_file)

for line in tast_list:
    releaserUrl = line['releaserUrl']
    platform = line['platform']
    if platform == "抖音":
        crawler_initialization = get_crawler(platform)
        try:
            crawler = crawler_initialization().get_releaser_page

            dic = crawler(releaserUrl)
            if dic:
                dic.update(line)
            rds3.hset(platform, releaserUrl, json.dumps(dic))
        except:
            continue
