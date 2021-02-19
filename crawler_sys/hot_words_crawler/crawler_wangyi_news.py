# -*- coding:utf-8 -*-
# @Time : 2020/3/2 11:07 
# @Author : litao
# -*- coding:utf-8 -*-
# @Time : 2020/2/28 12:09
# @Author : litao


import requests
import json, re, datetime, urllib
from crawler.crawler_sys.utils.output_results import retry_get_url
from crawler.crawler_sys.utils.output_results import hot_words_output_result, output_result
from crawler.crawler_sys.utils.trans_strtime_to_timestamp import trans_strtime_to_timestamp
from write_data_into_es.func_cal_doc_id import *
import base64
from crawler.crawler_sys.site_crawler.crawler_wangyi_news import Crawler_wangyi_news as Crawler_wy
crawler_qq_video_page = Crawler_wy().video_page


class Crawler_WangYi_News(object):
    def __init__(self):
        self.platform = "网易新闻"
        timestamp = int(datetime.datetime.now().timestamp() * 1e3)
        self.headers = {
                "data4-Sent-Millis": str(timestamp),
                "Add-To-Queue-Millis": str(timestamp),
                "User-D": "2zx5YfHmoBb72ayxYpQVUg==",
                "User-N": "HPcUw15+Yla9nvIP1c9vbqrHfvh/PCmpfK2DVDjsFFGhp4IV17bdU7hTwNc3Kfe3gVZiNSrnIe+bsZBFoMsbZQ==",
                "httpDNSIP": "101.71.145.130",
                "User-C": "5aS05p2h",
                "User-Agent": "NewsApp/32.1 Android/5.1.1 (OPPO/OPPO R11)",
                "X-NR-Trace-Id": "%s_355730601_866174725888628" % timestamp,
                "Host": "c.m.163.com",
                "Connection": "Keep-Alive",
                "Accept-Encoding": "gzip",
        }

    def get_hot_words(self):
        bulk_list = []
        url = "http://c.m.163.com/nc/search/hotWord.html"
        page_res = retry_get_url(url, headers=self.headers, proxies=3, timeout=5)
        page_json = page_res.json()
        for data in page_json["hotWordList"]:
            title = data["searchWord"]
            if title:
                dic = {
                        "platform": self.platform,
                        "title": title,
                        "fetch_time": int(datetime.datetime.now().timestamp() * 1e3),
                }
                bulk_list.append(dic)
        hot_words_output_result(bulk_list)
        return True


    def search_page(self,title):
        data_list = []
        encodestr = base64.b64encode(title.encode('utf-8'))
        encodestr = str(encodestr, 'utf-8')
        url = "http://c.m.163.com/search/comp2/Kg%3D%3D/20/{0}.html?".format(encodestr)
        para = "deviceId=2zx5YfHmoBb72ayxYpQVUg%3D%3D&version=newsclient.32.1.android&channel=VDEzNDg2NDc5MDkxMDc%3D&canal=bmV3c19sZl9jcGFfMg%3D%3D&dtype=0&tabname=shipin&position=5YiX6KGo6aG26YOo&ts={0}&sign=Di3opZw%2FFIPDdgreSK4VCKlnMSpm6FPoel5LeY88RgZ48ErR02zJ6%2FKXOnxX046I&spever=FALSE&open=scheme_%E9%BB%98%E8%AE%A4&openpath=/video/VT5O1KVCO".format(str(int(datetime.datetime.now().timestamp())))
        res = retry_get_url(url+para, headers=self.headers, timeout=5, proxies=3)
        page_text = res.json()
        for data in page_text["doc"]["result"]:
            print(data)
            data_list.append(data)
        output_result(result_Lst=data_list,
                      platform=self.platform,
                      output_to_es_raw=True,
                      )
        data_list.clear()
        ## sign和ts为加密字段 无法解决

    def get_hot_videos(self, max_page=10,**kwargs):
        pass


if __name__ == "__main__":
    crawler = Crawler_WangYi_News()
    crawler.get_hot_words()
    crawler.search_page("患者私自出院散步")
    # crawler.get_hot_videos("https://v.qq.com/x/search/?q=%E6%95%99%E8%82%B2%E9%83%A8%E5%9B%9E%E5%BA%94%E6%89%A9%E5%A4%A7%E7%A1%95%E5%A3%AB%E5%92%8C%E4%B8%93%E5%8D%87%E6%9C%AC%E6%8B%9B%E7%94%9F&stag=12",channel="教育部回应扩大硕士和专升本招生")