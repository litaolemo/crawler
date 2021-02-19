# -*- coding: utf-8 -*-
"""
Created on Wed May 16 12:00:37 2018

@author: hanye
"""

from crawler.crawler_sys.site_crawler import (crawler_toutiao,
                                              crawler_v_qq,
                                              crawler_iqiyi,
                                              crawler_youku,
                                              crawler_tudou,
                                              crawler_haokan,
                                              crawler_tencent_news,
                                              crawler_miaopai,
                                              crawler_pear,
                                              crawler_bilibili,
                                              crawler_mango,
                                              crawler_wangyi_news,
                                              crawler_kwai,
                                              crawler_douyin
                                              )

platform_crawler_reg = {
        'toutiao': crawler_toutiao.Crawler_toutiao,
        '腾讯视频': crawler_v_qq.Crawler_v_qq,
        'iqiyi': crawler_iqiyi.Crawler_iqiyi,
        'youku': crawler_youku.Crawler_youku,
        'new_tudou': crawler_tudou.Crawler_tudou,
        'haokan': crawler_haokan.Crawler_haokan,
        '腾讯新闻': crawler_tencent_news.Crawler_Tencent_News,
        'miaopai': crawler_miaopai.Crawler_miaopai,
        'pearvideo': crawler_pear.Crawler_pear,
        'bilibili': crawler_bilibili.Crawler_bilibili,
        'Mango': crawler_mango,
        "网易新闻": crawler_wangyi_news.Crawler_wangyi_news,
        "kwai": crawler_kwai.Crawler_kwai,
        '抖音': crawler_douyin.Crawler_douyin,
}


def get_crawler(platform):
    if platform in platform_crawler_reg:
        platform_crawler = platform_crawler_reg[platform]
    else:
        platform_crawler = None
        print("can't get crawler for platform %s, "
              "do we have the crawler for that platform?" % platform)
    return platform_crawler
