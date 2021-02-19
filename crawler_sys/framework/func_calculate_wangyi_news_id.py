# -*- coding:utf-8 -*-
# @Time : 2019/5/5 14:38 
# @Author : litao

import re


def calculate_wangyi_news_id(url):
    if "/sub/" in url:
        find_vid = re.findall('/sub/(.+)\.html', url)
    elif "/v/" in url:
        find_vid = re.findall('/v/(.+)\.html', url)
    else:
        return url

    if find_vid != []:
        vid = find_vid[0]
    else:
        vid = url
    return vid


if __name__=='__main__':
    print(calculate_wangyi_news_id("https://c.m.163.com/news/v/VA9LBOJ7S.html"))
    print(calculate_wangyi_news_id("https://c.m.163.com/news/sub/T1539761239294.html"))