# -*- coding:utf-8 -*-
# @Time : 2019/7/16 16:08 
# @Author : litao
# -*- coding:utf-8 -*-
# @Time : 2019/5/5 14:38
# @Author : litao

import re

def calculate_douyin_id(url):
    if "?" in url:
        find_vid = url.split("?")
    elif "video" in url:
        find_vid = re.findall('/video/(.*?)/', url)
        if find_vid:
            find_vid = ["https://www.iesdouyin.com/share/video/%s/" % find_vid[0]]
    else:
        return url

    if find_vid != []:
        vid = find_vid[0]
    else:
        vid = url
    return vid


if __name__=='__main__':
    print(calculate_douyin_id("https://www.iesdouyin.com/share/vido/6688242923181591821/?mid=6688519042262665996"))
    print(calculate_douyin_id("https://www.iesdouyin.com/share/video/6689249077596671245/?mid=6689052145968450308"))