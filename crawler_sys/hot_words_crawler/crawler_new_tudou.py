# -*- coding:utf-8 -*-
# @Time : 2020/2/25 15:23 
# @Author : litao
import requests
import json, re, datetime,urllib
from crawler.crawler_sys.utils.output_results import retry_get_url
from crawler.crawler_sys.utils.output_results import hot_words_output_result


class CrawlerNewTudou(object):
    def __init__(self):
        self.platform = "new_tudou"
        self.headers = {
                "Cookie": "",
                "User-Agent": "Tudou;6.39.1;Android;5.1.1;OPPO R11",
                "Accept-Encoding": "gzip,deflate",
                "Connection": "close",
                "Host": "apis.tudou.com",
        }
    def get_hot_words(self):
        bulk_list = []

        url = "https://apis.tudou.com/search/v1/hot?_t_={0}&e=md5&_s_=9a4abf3a92efad0605f8e31481327014&operator=CHINA+MOBILE_46007&network=WIFI".format(
            int(datetime.datetime.now().timestamp()))
        res = retry_get_url(url,proxies=3,headers=self.headers)
        res_json = res.json()
        for title in res_json["result"]["search"]["data"]:
            dic = {
                    "platform": self.platform,
                    "title": title["keyword"],
                    "fetch_time":int(datetime.datetime.now().timestamp()*1e3)
            }
            bulk_list.append(dic)
        hot_words_output_result(bulk_list)
        return True

    def get_hot_videos(self,title=None,max_page=10,**kwargs):
        page = 1
        while page <= max_page:
            get_dic = {
                    "keyword": title,
                    # "pid": "6c23a6957198fad2",
                    # "guid": "2139ff131a8a7d9ef7d3014cc8b97010",
                    "mac": "",
                    "imei": "null",
                    "ver": "6.39.1",
                    "_t_": int(datetime.datetime.now().timestamp()),
                    "e": "md5",
                    # "_s_": "b905d3a9738d7d2f815687428563d8f7",
                    "operator": "CHINA+MOBILE_46007",
                    "network": "WIFI",
                    "ftype": "0",
                    "cateId": "0",
                    "seconds": "0",
                    "seconds_end": "0",
                    "ob": "",
                    "pg": str(page),
                    "pz": "30",
                    # "aaid": "1.58259884569785E+20",
                    "brand": "OPPO",
                    "btype": "OPPO+R11",
                    "sdkver": "2",
                    "apad": "0",
                    # "utdid": "XkjV9GsfBysDACyQ2%2BiF8MOw",
                    "srid": "1",
                    "userType": "guest",
            }
            requests_res = retry_get_url("https://apis.tudou.com/search/v2/integration?%s"%urllib.parse.urlencode(get_dic),headers=self.headers,proxies=3)
            requests_json = requests_res.json()
            page += 1
            print(requests_json)
            for data in requests_json["results"]["ugc"]["data"]:
                print(data)


if __name__ == "__main__":
    crawler = CrawlerNewTudou()
    # crawler.get_hot_words()
    crawler.get_hot_videos("范冰冰蛋糕裙")