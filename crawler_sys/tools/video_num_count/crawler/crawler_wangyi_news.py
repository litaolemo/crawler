# -*- coding:utf-8 -*-
# @Time : 2019/5/5 9:48 
# @Author : litao

import os
import re
import time
import copy
import requests
import datetime
import pandas as pd
import json
from bs4 import BeautifulSoup
import urllib
try:
    from .func_get_releaser_id import *
except:
    from func_get_releaser_id import *

class Crawler_wangyi_news():

    def __init__(self, timeout=None, platform='网易新闻'):
        if timeout == None:
            self.timeout = 10
        else:
            self.timeout = timeout
        self.platform = platform
        self.TotalVideo_num = None
        self.midstepurl = None
        self.video_data = {}
        self.video_data['platform'] = self.platform
        self.headers = {
            "Accept-Encoding": "gzip",
            "Connection": "keep-alive",
            "Host": "c.m.163.com",
            "User-Agent": "User-Agent: NewsApp/34.1.1 Android/6.0.1 (vivo/VIVO X20 Plus)",
            "Add-To-Queue-Millis": str(int(datetime.datetime.now().timestamp()*1e4)),
            "data4-Sent-Millis": str(int(datetime.datetime.now().timestamp()*1e4))
        }


    def get_releaser_id(self,url):
        return get_releaser_id(platform=self.platform,releaserUrl=url)

    def forllower_num_to_int(self,num):
        try:
            res = int(num)
            return res
        except:
            if str(num)[-1:] == "万":
                return int(float(num[:-1])*1e4)
            elif isinstance(num,float):
                return num
            elif isinstance(num, int):
                return num


    def get_releaser_follower_num(self,releaserUrl):
        releaser_id = self.get_releaser_id(releaserUrl)
        url = "http://c.m.163.com/nc/subscribe/v2/topic/%s.html" % releaser_id
        res = requests.get(url,headers=self.headers)
        res_json = res.json()
        try:
            follower_num = self.forllower_num_to_int(res_json.get("subscribe_info").get("subnum"))
            print('%s follower number is %s' % (releaserUrl, follower_num))
            return follower_num
        except:
            print("can't can followers")

    def one_video_page(self, skipID):
        # onehead = {
        #     "Accept-Encoding": "gzip",
        #     "Connection": "keep-alive",
        #     "Host": "c.m.163.com",
        #     "User-Agent": "NewsApp/33.2.1 Android/6.0.1 (HUAWEI/BLA-AL00)",
        #     "data4-Sent-Millis": str(int(datetime.datetime.now().timestamp())),
        #     "Add-To-Queue-Millis": str(int(datetime.datetime.now().timestamp())),
        #     "User-D": "sklfRdL61S9GUQ4M7DSzdvA6U6LFEZr0pAEonUVTJrYHNFmgkLuyUgNU6zUV7MVx",
        #     "User-N": "5YYRIR130q3XkGjfXOlxDRb3VOTQjtndD5Qg328+Wm1OiOmXxQJ2MFkIjnBMtM41",
        #     "httpDNSIP": "59.111.160.220",
        #     "User-C": "5aS05p2h",
        #     "X-NR-Trace-Id": "1557909701808_152529648_CQk3MzEzYWU3MWRmOWU1MzY3CVpYMUc0MkNQSkQ%3D"
        # }
        # josn1 = [{"s":"hhgdge1557909486361","u":"CQk3MzEzYWU3MWRmOWU1MzY3CVpYMUc0MkNQSkQ%3D","p":"","id":"2x1kfBk63z","e":[{"n":"COLUMNX","g":"头条","ts":1557909701996,"t":1},{"n":"VA","kv":{"id":"VWE9F1MEU","action":"暂停","du":1},"ts":1557909704140,"t":1},{"n":"_vvX","kv":{"pg":0.008414382,"referer_id":"VWE9F1MEU","columnd":"T1525338599542","id":"VWE9F1MEU","domain":"flv0.bn.netease.com","loaddu":181,"schsession":"fa38e72f-1b6f-473e-b76e-310db69ca689","column":"头条","from":"详情页"},"ts":1557911053464,"du":1351084,"pg":0.008414382,"t":1}]}]
        # news1 = requests.post(" http://m.analytics.126.net/news/c",json=josn1)
        # json2 = [{"s":"hhgdge1557909486361","u":"CQk3MzEzYWU3MWRmOWU1MzY3CVpYMUc0MkNQSkQ%3D","p":"","id":"2x1kfBk63z","e":[{"n":"EV","kv":{"offsets":"1,2,3,4,5,6,7,8,9,10,11,12,15,14,9,10,11,12,13","types":"video,video,video,video,video,video,video,video,video,video,video,video,video,video,video,video,video,video,video","columnd":"T1525338599542","id":"1557909701984","ids":"VW88QQDRI,VACSF7B1A,VMCEBBGNV,VVDQR7L1R,VW24VSFVP,VW59M2OKM,VWCRFJRJN,VW59LU346,VU2NQBPHT,VW2S708M9,VZBINK7UI,VW5C794NM,VW59M2EK1,VW4Q7LEM8,VU2NQBPHT,VW2S708M9,VZBINK7UI,VW5C794NM,VWA92C5SN","column":"头条","from":"详情页","dus":"1323537,1323589,1335172,1335186,13009,16274,16271,4680,4993,5008,9603,9582,10193,10245,865,905,1240,1278,11475","fromID":"VWE9F1MEU"},"ts":1557911053549,"t":1}]}]
        # news2 = requests.post(" http://m.analytics.126.net/news/c",json=json2)
        # print(news1.text)
        release_url = "https://c.m.163.com/nc/video/detail/%s.html" % skipID
        web_url = "https://c.m.163.com/news/v/%s.html" % skipID
        res = requests.get(release_url, headers=self.headers)
        # print(res.text)
        page_dic = res.json()
        return page_dic, web_url

    def video_page(self, url=None,
                   output_to_file=False,
                   filepath=None,
                   releaser_page_num_max=30,
                   output_to_es_raw=False,
                   es_index=None,
                   doc_type=None,
                   output_to_es_register=False,
                   push_to_redis=False, *args):

        """
        get video info from api instead of web page html
        the most scroll page is 1000
        """
        releaser = ""
        count = 1
        result_list = []
        page_count = 0
        size_num = 0
        # releaser_id = self.get_releaser_id(url)
        while count < releaser_page_num_max:
            if size_num > 1000:
                size_num = 0

            size_num += 20
            count += 1
            url_dic = {'channel': 'T1457068979049', 'subtab': 'Video_Recom', 'size': "10", 'offset': size_num,
                       'fn': '3', 'devId': 'sklfRdL61S9GUQ4M7DSzdvA6U6LFEZr0pAEonUVTJrYHNFmgkLuyUgNU6zUV7MVx',
                       'version': '33.2.1', 'net': 'wifi', 'ts': '1557126556',
                       'sign': 'YTk73p++NeCfCJRpZkThWxGYX0gVcFWjUVLCRIRwftV48ErR02zJ6/KXOnxX046I', 'encryption': '1',
                       'canal': 'lite_wifi_cpa10', 'mac': 'racUMC0A9havm+He6jH3YAvVdjgSXYDtwEDZ03eH1l8='}

            releaserUrl = 'https://c.m.163.com/recommend/getChanListNews?%s' % urllib.parse.urlencode(url_dic)
            # print(releaserUrl)
            page_count += 20
            get_page = requests.get(releaserUrl, headers=self.headers)
            page_dic = get_page.json()
            data_list = page_dic.get("视频")
            # print(data_list)
            # print(releaserUrl)
            if data_list == []:
                print("no more data at releaser: %s page: %s " % (releaser, count))
                pcursor = "no_more"
                continue
            else:
                print("get data at  page: %s" % (count))

                for info_dic in data_list:
                    skipID = info_dic.get("vid")
                    video_dic = copy.deepcopy(self.video_data)
                    video_dic['title'] = info_dic.get('title')
                    video_dic['url'] = "https://c.m.163.com/news/v/%s.html" % skipID
                    video_dic['releaser'] = info_dic.get('topicName')
                    video_dic['releaserUrl'] = "https://c.m.163.com/news/sub/%s.html" % info_dic.get("videoTopic").get(
                        "tid")
                    video_dic['releaser_id_str'] = "网易新闻_%s" % self.get_releaser_id(video_dic['releaserUrl'] )
                    video_dic['release_time'] = int(
                        datetime.datetime.strptime(info_dic.get('ptime'), "%Y-%m-%d %H:%M:%S").timestamp() * 1e3)
                    video_dic['play_count'] = info_dic.get("playCount")
                    video_dic['comment_count'] = info_dic.get('replyCount')
                    video_dic['favorite_count'] = info_dic.get('voteCount')
                    if not video_dic['play_count']:
                        video_dic['play_count'] = 0
                    if not video_dic['favorite_count']:
                        video_dic['favorite_count'] = 0
                    video_dic['video_id'] = skipID
                    video_dic['fetch_time'] = int(time.time() * 1e3)
                    video_dic['duration'] = info_dic.get("length")


                    result_list.append(video_dic)
                    if len(result_list) >= 100:
                        output_result(result_Lst=result_list,
                                      platform=self.platform,
                                      output_to_file=output_to_file,
                                      filepath=filepath,
                                      output_to_es_raw=output_to_es_raw,
                                      es_index=es_index,
                                      doc_type=doc_type,
                                      output_to_es_register=output_to_es_register)
                        # print((result_list))
                        result_list.clear()
        if result_list != []:
            output_result(result_Lst=result_list,
                          platform=self.platform,
                          output_to_file=output_to_file,
                          filepath=filepath,
                          output_to_es_raw=output_to_es_raw,
                          es_index=es_index,
                          doc_type=doc_type,
                          output_to_es_register=output_to_es_register)
            # print((result_list))
            result_list.clear()
        return result_list

    #    @logged
    def releaser_page(self, releaserUrl,
                      output_to_file=False,
                      filepath=None,
                      releaser_page_num_max=10000,
                      output_to_es_raw=False,
                      es_index=None,
                      doc_type=None,
                      output_to_es_register=False,
                      push_to_redis=False):

        """
        get video info from api instead of web page html
        the most scroll page is 1000
        """
        releaser = ""
        count = 1
        result_list = []
        releaser_id = self.get_releaser_id(releaserUrl)
        page_count = 0
        releaserUrl_name = 'http://c.m.163.com/nc/subscribe/list/%s/video/%s-20.html' % (releaser_id, page_count)
        pcursor = None
        self.video_data['releaserUrl'] = releaserUrl
        # proxies = {
        #         'http': "http://114.246.64.146:9999",
        #
        # }

        while count <= releaser_page_num_max and count <= 1000 and pcursor != "no_more":
            count_false = 0
            releaserUrl = 'http://c.m.163.com/nc/subscribe/list/%s/video/%s-20.html' % (releaser_id, page_count)

            get_page = requests.get(releaserUrl, headers=self.headers,timeout=2)
            try:
                page_dic = get_page.json()
                data_list = page_dic.get("tab_list")
            except:
                if count_false < 5:
                    continue
                else:
                    data_list =[]
            # print(data_list)
            # print(releaserUrl)
            page_count += 20
            if data_list == []:
                print("no more data at releaser: %s page: %s " % (releaser, count))
                pcursor = "no_more"
                continue
            else:
                print("get data at releaser: %s page: %s" % (releaser, count))
                count += 1
                for info_dic in data_list:
                    skipID = info_dic.get("skipID")
                    page_data, release_url = self.one_video_page(skipID)
                    video_dic = copy.deepcopy(self.video_data)
                    video_dic['title'] = page_data.get('title')
                    video_dic['url'] = release_url
                    video_dic['releaser'] = page_data.get('topicName')
                    video_dic['releaserUrl'] = releaserUrl_name
                    video_dic['release_time'] = int(
                        datetime.datetime.strptime(info_dic.get('ptime'), "%Y-%m-%d %H:%M:%S").timestamp() * 1e3)
                    video_dic['play_count'] = page_data.get("playCount")
                    if not video_dic['play_count']:
                        video_dic['play_count'] = 0
                    video_dic['favorite_count'] = page_data.get('voteCount')
                    if not video_dic['favorite_count']:
                        video_dic['favorite_count'] = 0
                    video_dic['comment_count'] = page_data.get('replyCount')
                    video_dic['video_id'] = skipID
                    video_dic['fetch_time'] = int(time.time() * 1e3)
                    video_dic['duration'] = page_data.get("length")
                    video_dic['releaser_id_str'] = "网易新闻_%s" % releaser_id
                    yield video_dic


    def time_range_video_num(self, start_time, end_time, url_list):
        data_lis = []
        info_lis = []
        columns = ["title","url","release_time","releaserUrl","duration"]
        dur_count = 0
        count_false = 0
        for dic in url_list:
            for res in self.releaser_page(dic["url"]):
                title = res["title"]
                link = res["url"]
                video_time = res["release_time"]
                video_time_str = datetime.datetime.fromtimestamp(video_time / 1000).strftime("%Y-%m-%d %H-%M-%S")
                print(res)

                if video_time:
                    if start_time < video_time:
                        if video_time < end_time:
                            data_lis.append((title, link, video_time_str, dic["url"],res["duration"]))
                            if int(res["duration"]) <= 600:
                                dur_count += 1
                    else:
                        count_false += 1
                        if count_false > 30:
                            break
                        else:
                            continue
            csv_save = pd.DataFrame(data_lis)
            if data_lis:
                try:
                    csv_save.to_csv("%s.csv" % (dic["platform"] + "_" + dic["releaser"]), encoding="gb18030",header=columns)
                except:
                    csv_save.to_csv("%s.csv" % (dic["platform"] + "_" + dic["releaser"]), encoding="utf-8",
                                    header=columns)
            info_lis.append([dic["platform"], dic["releaser"], len(data_lis),dur_count])
            data_lis = []

        csv_save = pd.DataFrame(info_lis)
        csv_save.to_csv("%s.csv" % (datetime.datetime.now().strftime("%Y-%m-%d")), encoding="gb18030", mode='a',
                        header=None, index=None)

if __name__ == '__main__':
    test = Crawler_wangyi_news()
    # url = 'https://live.kuaishou.com/profile/IIloveyoubaby'
    user_lis = ["https://c.m.163.com/news/sub/T1531895288972.html"]
    url_lis = [
            {"platform":"网易新闻",
             "url": "https://c.m.163.com/news/sub/T1512044479072.html",
             "releaser":"澎湃新闻"
             },
    ]
    start_time = 1562559137000
    end = 1562584337789
    test.time_range_video_num(start_time, end, url_lis)