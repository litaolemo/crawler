# -*- coding:utf-8 -*-
# @Time : 2019/9/19 10:25 
# @Author : litao
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 26 21:04:19 2018

@author: fangyucheng
"""

import os
import re
import random
import copy
import json
import time, datetime
import requests
import urllib
import random
from urllib import parse
from urllib.parse import urlencode
from crawler_sys.utils.output_results import output_result
from crawler_sys.framework.video_fields_std import Std_fields_video
from crawler_sys.utils.trans_strtime_to_timestamp import trans_strtime_to_timestamp
from crawler_sys.utils.trans_duration_str_to_second import trans_duration
from crawler.crawler_sys.utils.util_logging import logged

try:
    from crawler_sys.framework.func_get_releaser_id import *
except:
    from func_get_releaser_id import *
from crawler.crawler_sys.utils.trans_duration_str_to_second import *
from crawler.crawler_sys.proxy_pool.func_get_proxy_form_kuaidaili import get_proxy

class Crawler_baijiahao():

    def __init__(self):
        self.platfrom = 'haokan'
        self.video_data_template = Std_fields_video().video_data
        self.video_data_template['platform'] = 'haokan'
        self.count_false = 0
        pop_lst = ['channel', 'describe', 'isOriginal', 'repost_count']
        for key in pop_lst:
            self.video_data_template.pop(key)


    def get_releaser_id(self, releaserUrl):
        return get_releaser_id(platform=self.platfrom, releaserUrl=releaserUrl)

    def get_releaser_follower_num(self, releaserUrl):
        proxies = get_proxy(1)
        releaser_id = self.get_releaser_id(releaserUrl)
        url = "https://sv.baidu.com/haokan/api?cmd=baijia/authorInfo&log=vhk&tn=1008621v&ctn=1008621v&imei=&cuid=51BF00514510A03B32E6CA9D7443D8F8|504550857697800&bdboxcuid=&os=android&osbranch=a0&ua=810_1440_270&ut=MI%20NOTE%203_6.0.1_23_Xiaomi&apiv=4.6.0.0&appv=414011&version=4.14.1.10&life=1555296294&clife=1558350548&hid=02112F128209DD6BAF39CA37DE9C05E6&imsi=0&network=1&location={%22prov%22:%22%22,%22city%22:%22%22,%22county%22:%22%22,%22street%22:%22%22,%22latitude%22:39.911017,%22longitude%22:116.413562}&sids=1957_2-2193_3-2230_4-2320_1-2326_2-2353_1-2359_3-2376_1-2391_1-2433_4-2436_5-2438_1-2442_1-2443_2-2452_1-2457_2-2470_1-2480_2-2511_1-2525_4-2529_1-2537_1-2538_1-2540_1-2555_2-2563_1-2565_2-2568_1-2574_1-2575_1-2577_1-2582_1"
        headers = {
                "Host": "sv.baidu.com",
                "Connection": "keep-alive",
                "Content-Length": "60",
                "Charset": "UTF-8",
                "User-Agent": 'Mozilla/5.0 (Linux; Android 6.0.1; MI NOTE 3 Build/V417IR; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/52.0.2743.100 Mobile Safari/537.36 haokan/4.14.1.10 (Baidu; P1 6.0.1)/imoaiX_32_1.0.6_3+ETON+IM/1008621v/51BF00514520A03B32E6CA9D7443D8F8%7C504550857697800/1/4.14.1.10/414011/1',
                "X-Bfe-Quic": "enable=1",
                "XRAY-REQ-FUNC-ST-DNS": "okHttp;1558350575755;0",
                "XRAY-TRACEID": "be54291d-c13a-4a88-8337-9e70ad75d7d8",
                # "Cookie": "BAIDUID=13CD01ABA9F3F112EEE8880716798F35:FG=1; BAIDUZID=T0WvNv-W1ew-E8YpKuV10jpN5j2DYGOE4bUCyiIsT3Iun8dpVe7GQpFr7mFjzYFHEFOhQgdC_ixxf48KpG1iwQb7HiU9ypKA2obES0JACE_E; BAIDUCUID=gaHRu_u_v8gga2830u2uu_uCHilEi-uk_av9i0PDHtifa28fga26fgayvf_NP2ijA",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept-Encoding": "gzip, deflate"
        }
        post_dic = {"baijia/authorInfo": "method=get&app_id=%s" % releaser_id}
        get_page = requests.post(url, data=post_dic, headers=headers,proxies=proxies)
        res = get_page.json()
        try:
            follower_num = res.get("baijia/authorInfo").get("data").get("fansCnt")
            print('%s follower number is %s' % (releaserUrl, follower_num))
            releaser_img = self.get_releaser_image(data=res)
            return follower_num,releaser_img
        except:
            print("can't can followers")


    def get_releaser_image(self,releaserUrl=None,data=None):
        if releaserUrl:
            proxies = get_proxy(1)
            releaser_id = self.get_releaser_id(releaserUrl)
            url = "https://sv.baidu.com/haokan/api?cmd=baijia/authorInfo&log=vhk&tn=1008621v&ctn=1008621v&imei=&cuid=51BF00514510A03B32E6CA9D7443D8F8|504550857697800&bdboxcuid=&os=android&osbranch=a0&ua=810_1440_270&ut=MI%20NOTE%203_6.0.1_23_Xiaomi&apiv=4.6.0.0&appv=414011&version=4.14.1.10&life=1555296294&clife=1558350548&hid=02112F128209DD6BAF39CA37DE9C05E6&imsi=0&network=1&location={%22prov%22:%22%22,%22city%22:%22%22,%22county%22:%22%22,%22street%22:%22%22,%22latitude%22:39.911017,%22longitude%22:116.413562}&sids=1957_2-2193_3-2230_4-2320_1-2326_2-2353_1-2359_3-2376_1-2391_1-2433_4-2436_5-2438_1-2442_1-2443_2-2452_1-2457_2-2470_1-2480_2-2511_1-2525_4-2529_1-2537_1-2538_1-2540_1-2555_2-2563_1-2565_2-2568_1-2574_1-2575_1-2577_1-2582_1"
            headers = {
                    "Host": "sv.baidu.com",
                    "Connection": "keep-alive",
                    "Content-Length": "60",
                    "Charset": "UTF-8",
                    "User-Agent": 'Mozilla/5.0 (Linux; Android 6.0.1; MI NOTE 3 Build/V417IR; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/52.0.2743.100 Mobile Safari/537.36 haokan/4.14.1.10 (Baidu; P1 6.0.1)/imoaiX_32_1.0.6_3+ETON+IM/1008621v/51BF00514520A03B32E6CA9D7443D8F8%7C504550857697800/1/4.14.1.10/414011/1',
                    "X-Bfe-Quic": "enable=1",
                    "XRAY-REQ-FUNC-ST-DNS": "okHttp;1558350575755;0",
                    "XRAY-TRACEID": "be54291d-c13a-4a88-8337-9e70ad75d7d8",
                    # "Cookie": "BAIDUID=13CD01ABA9F3F112EEE8880716798F35:FG=1; BAIDUZID=T0WvNv-W1ew-E8YpKuV10jpN5j2DYGOE4bUCyiIsT3Iun8dpVe7GQpFr7mFjzYFHEFOhQgdC_ixxf48KpG1iwQb7HiU9ypKA2obES0JACE_E; BAIDUCUID=gaHRu_u_v8gga2830u2uu_uCHilEi-uk_av9i0PDHtifa28fga26fgayvf_NP2ijA",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept-Encoding": "gzip, deflate"
            }
            post_dic = {"baijia/authorInfo": "method=get&app_id=%s" % releaser_id}
            get_page = requests.post(url, data=post_dic, headers=headers, proxies=proxies,timeout=3)
            res = get_page.json()
            try:
                releaser_img_url = res.get("baijia/authorInfo").get("data").get("avatar")
                return releaser_img_url
            except:
                print("can't get releaser_img")
        else:
            releaser_img_url = data.get("baijia/authorInfo").get("data").get("avatar")
            return releaser_img_url

    def releaser_page_web_by_time(self, releaserUrl,
                          output_to_file=False,
                          filepath=None,
                          releaser_page_num_max=100000,
                          output_to_es_raw=False,
                          output_to_es_register=False,
                          push_to_redis=False,
                          es_index=None,
                          doc_type=None,
                          fetchFavoriteCommnt=True,proxies_num=None):

        releaser_id = get_releaser_id(platform=self.platfrom, releaserUrl=releaserUrl)
        proxies = get_proxy(proxies_num)
        result_lst = []
        url = 'https://webpage.mbd.baidu.com/home?context={%22app_id%22:%22' + releaser_id + "%22}"
        headers = {
                "Host": "webpage.mbd.baidu.com",
                "Connection": "keep-alive",
                "Cache-Control": "max-age=0",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-User": "?1",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "Sec-Fetch-Site": "none",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh,zh-CN;q=0.9"
        }
        video_list_headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh,zh-CN;q=0.9",
                "Cache-Control": "max-age=0",
                "Referer": url,
                "Host": "mbd.baidu.com",
                "Sec-Fetch-Mode": "no-cors",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36"
        }
        def get_page_info():
            page_info = requests.get(url, headers=headers,proxies=proxies,timeout=5)
            #print(page_info.text)
            page_text = page_info.text
            cookies = page_info.cookies
            res_data = re.findall("window.runtime= (.*),window.runtime.pageType", page_text)
            if res_data:
                res_data = json.loads(res_data[0])
                #print(res_data)
            return res_data, cookies

        res_data, cookies = get_page_info()
        ctime = None
        releaser_fans = res_data.get("user").get("fans_num")
        uk = res_data.get("user").get("uk")
        releaser = res_data.get("user").get("display_name")
        Hmery_Time = cookies["Hmery-Time"]
        page_num = 0
        count_false = 0
        has_more = True
        def get_data_list(uk, releaser, Hmery_Time, ctime):
            interact_list = []
            data_list_dic = {}
            data_dic = {
                    "tab": "video",
                    "num": "10",
                    "uk": uk,
                    "type": "newhome",
                    "action": "dynamic",
                    "format": "jsonp",
                    "Tenger-Mhor": Hmery_Time,
            }

            if ctime:
                data_dic["ctime"] = ctime
            video_list_url = "https://mbd.baidu.com/webpage?%s" % urlencode(data_dic)
            video_list_res = requests.get(video_list_url, headers=video_list_headers, cookies=cookies,proxies=proxies,timeout=5)
            video_list_res_json = json.loads(re.findall("\((.*)\)", video_list_res.text)[0])
            data_lis = video_list_res_json.get("data").get("list")
            has_more = video_list_res_json.get("data").get("hasMore")
            ctime = video_list_res_json.get("data").get("query").get("ctime")
            #interact_dic = {}
            data_dic = {}
            if data_lis:
                for single_data in data_lis:
                    # interact_dic["user_type"] = single_data["user_type"]
                    # interact_dic["dynamic_id"] = single_data["dynamic_id"]
                    # interact_dic["feed_id"] = single_data["feed_id"]
                    # interact_dic["dynamic_type"] = single_data["dynamic_type"]
                    # interact_dic["dynamic_sub_type"] = single_data["dynamic_sub_type"]
                    _id = single_data["id"]
                    #single_data["asyncParams"]["thread_id"] = str(single_data["asyncParams"]["thread_id"])
                    interact_list.append(single_data["asyncParams"])
                    data_dic["title"] = single_data["itemData"]["title"]
                    data_dic["baijiahao_url"] = single_data["itemData"]["url"]
                    data_dic["url"] = "https://haokan.baidu.com/v?vid=%s" % single_data["dynamic_id"]
                    data_dic["platform"] = self.platfrom
                    data_dic["fetch_time"] = int(datetime.datetime.now().timestamp()*1e3)
                    data_dic["release_time"] = int(single_data["itemData"]["updated_at"]*1e3)
                    data_dic["data_provider"] = "CCR"
                    data_dic["releaser"] = releaser
                    data_dic["data_sources"] = "baijiahao"
                    data_dic["releaserUrl"] = url
                    data_dic["video_id"] = single_data["dynamic_id"]
                    data_dic["releaser_id_str"] = "haokan_%s" % releaser_id
                    data_dic["duration"] = trans_duration(single_data["itemData"]["duration"])
                    data_dic['releaser_followers_count'] = int(releaser_fans)
                    data_dic["video_img"] = single_data["itemData"]["imgSrc"][0]["src"]
                    one_dic = None
                    one_dic = copy.deepcopy(data_dic)
                    data_list_dic[_id]=one_dic
            return interact_list,data_list_dic,ctime,has_more

        def get_interact_list(uk,Hmery_Time,interact_list):
            interact_dic = {
                    "uk": uk,
                    "type": "homepage",
                    "action": "interact",
                    "format": "jsonp",
                    "Tenger-Mhor": Hmery_Time,
                    #"params":urlencode(str(interact_list).replace)
            }
            params_str = str(interact_list).replace("'",'"').replace(" ","")
            url = "https://mbd.baidu.com/webpage?%s&params=%s" % (urlencode(interact_dic),params_str)
            interact_res = requests.get(url,headers=video_list_headers,cookies=cookies,proxies=proxies,timeout=5)
            interact_res_json = json.loads(re.findall("\((.*)\)", interact_res.text)[0])
            return interact_res_json["data"]["user_list"]

        while page_num <= releaser_page_num_max and has_more and count_false <= 5:
            try:
                proxies = get_proxy(proxies_num)
                interact_list,data_list_dic,ctime,has_more = get_data_list(uk, releaser, Hmery_Time, ctime)
                interact_res_json = get_interact_list(uk,Hmery_Time,interact_list)
                page_num += 1
                for interact in interact_res_json:
                    data_list_dic[interact]["comment_count"] = int(interact_res_json[interact]["comment_num"])
                    data_list_dic[interact]["favorite_count"] = int(interact_res_json[interact]["praise_num"])
                    data_list_dic[interact]["play_count"] = int(interact_res_json[interact]["read_num"])
                    data_list_dic[interact]["repost_count"] = int(interact_res_json[interact]["forward_num"])
                    yield data_list_dic[interact]
            except:
                proxies = get_proxy(1)
                count_false += 1
                if count_false <= 5:
                    continue
                else:
                    break


    def releaser_dynamic_page_web_by_time(self, releaserUrl,
                          output_to_file=False,
                          filepath=None,
                          releaser_page_num_max=5000,
                          output_to_es_raw=False,
                          output_to_es_register=False,
                          push_to_redis=False,
                          es_index=None,
                          doc_type=None,
                          fetchFavoriteCommnt=True,proxies_num=None):

        releaser_id = get_releaser_id(platform=self.platfrom, releaserUrl=releaserUrl)
        proxies = get_proxy(proxies_num)
        result_lst = []
        url = 'https://webpage.mbd.baidu.com/home?context={%22app_id%22:%22' + releaser_id + "%22}"
        headers = {
                "Host": "webpage.mbd.baidu.com",
                "Connection": "keep-alive",
                "Cache-Control": "max-age=0",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-User": "?1",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "Sec-Fetch-Site": "none",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh,zh-CN;q=0.9"
        }
        video_list_headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh,zh-CN;q=0.9",
                "Cache-Control": "max-age=0",
                "Referer": url,
                "Host": "mbd.baidu.com",
                "Sec-Fetch-Mode": "no-cors",
                "Sec-Fetch-Site": "same-site",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36"
        }
        def get_page_info():
            page_info = requests.get(url, headers=headers,proxies=proxies,timeout=5)
            #print(page_info.text)
            page_text = page_info.text
            cookies = page_info.cookies
            res_data = re.findall("window.runtime= (.*),window.runtime.pageType", page_text)
            if res_data:
                res_data = json.loads(res_data[0])
                #print(res_data)
            return res_data, cookies

        res_data, cookies = get_page_info()
        ctime = None
        releaser_fans = res_data.get("user").get("fans_num")
        uk = res_data.get("user").get("uk")
        releaser = res_data.get("user").get("display_name")
        Hmery_Time = cookies["Hmery-Time"]
        page_num = 0
        count_false = 0
        has_more = True

        def get_data_list(uk, releaser, Hmery_Time, ctime):
            interact_list = []
            data_list_dic = {}
            data_dic = {
                    "tab": "dynamic",
                    "num": "10",
                    "uk": uk,
                    "type": "newhome",
                    "action": "dynamic",
                    "format": "jsonp",
                    "Tenger-Mhor": Hmery_Time,
            }

            if ctime:
                data_dic["ctime"] = ctime
            video_list_url = "https://mbd.baidu.com/webpage?%s" % urlencode(data_dic)
            video_list_res = requests.get(video_list_url, headers=video_list_headers, cookies=cookies,proxies=proxies,timeout=5)
            video_list_res_json = json.loads(re.findall("\((.*)\)", video_list_res.text)[0])
            data_lis = video_list_res_json.get("data").get("list")
            has_more = video_list_res_json.get("data").get("hasMore")
            ctime = video_list_res_json.get("data").get("query").get("ctime")
            #interact_dic = {}
            data_dic = {}
            if data_lis:
                for single_data in data_lis:
                    # interact_dic["user_type"] = single_data["user_type"]
                    # interact_dic["dynamic_id"] = single_data["dynamic_id"]
                    # interact_dic["feed_id"] = single_data["feed_id"]
                    # interact_dic["dynamic_type"] = single_data["dynamic_type"]
                    # interact_dic["dynamic_sub_type"] = single_data["dynamic_sub_type"]
                    if single_data["itemData"]["layout"] == "video_play":
                        #print(single_data["itemData"]["media_type"])
                        pass
                    else:
                        #print(single_data["itemData"]["layout"],single_data["itemData"]["media_type"])
                        continue
                    _id = single_data["id"]
                    #single_data["asyncParams"]["thread_id"] = str(single_data["asyncParams"]["thread_id"])
                    interact_list.append(single_data["asyncParams"])
                    data_dic["title"] = single_data["itemData"]["title"]
                    data_dic["baijiahao_url"] = single_data["itemData"]["feed_url"]
                    data_dic["url"] = "https://haokan.baidu.com/v?vid=%s" % single_data["feed_id"]
                    data_dic["platform"] = self.platfrom
                    data_dic["fetch_time"] = int(datetime.datetime.now().timestamp()*1e3)
                    data_dic["release_time"] = int(single_data["itemData"]["ctime"]*1e3)
                    data_dic["data_provider"] = "CCR"
                    data_dic["releaser"] = releaser
                    data_dic["data_sources"] = "baijiahao"
                    data_dic["releaserUrl"] = url
                    data_dic["video_id"] = single_data["dynamic_id"]
                    data_dic["releaser_id_str"] = "haokan_%s" % releaser_id
                    data_dic["duration"] = int(float(trans_duration(single_data["itemData"]["duration"])))
                    data_dic['releaser_followers_count'] = int(releaser_fans)
                    data_dic["video_img"] = single_data["itemData"]["img_400_200"]
                    one_dic = None
                    one_dic = copy.deepcopy(data_dic)
                    data_list_dic[_id]=one_dic
            return interact_list,data_list_dic,ctime,has_more

        def get_interact_list(uk,Hmery_Time,interact_list):
            interact_dic = {
                    "uk": uk,
                    "type": "homepage",
                    "action": "interact",
                    "format": "jsonp",
                    "Tenger-Mhor": Hmery_Time,
                    #"params":urlencode(str(interact_list).replace)
            }
            params_str = str(interact_list).replace("'",'"').replace(" ","")
            url = "https://mbd.baidu.com/webpage?%s&params=%s" % (urlencode(interact_dic),params_str)
            interact_res = requests.get(url,headers=video_list_headers,cookies=cookies,proxies=proxies,timeout=3)
            interact_res_json = json.loads(re.findall("\((.*)\)", interact_res.text)[0])
            return interact_res_json["data"]["user_list"]

        while page_num <= releaser_page_num_max and has_more and count_false <=5:
            try:
                proxies = get_proxy(proxies_num)
                interact_list,data_list_dic,ctime,has_more = get_data_list(uk, releaser, Hmery_Time, ctime)
                if interact_list:
                    interact_res_json = get_interact_list(uk,Hmery_Time,interact_list)
                else:
                    continue
                page_num += 1
                for interact in interact_res_json:
                    data_list_dic[interact]["comment_count"] = int(interact_res_json[interact]["comment_num"])
                    data_list_dic[interact]["favorite_count"] = int(interact_res_json[interact]["praise_num"])
                    data_list_dic[interact]["play_count"] = int(interact_res_json[interact]["read_num"])
                    data_list_dic[interact]["repost_count"] = int(interact_res_json[interact]["forward_num"])
                    yield data_list_dic[interact]
            except Exception as e:
                print(e)
                proxies = get_proxy(1)
                count_false += 1
                if count_false <= 5:
                    continue
                else:
                    break





if __name__ == '__main__':
    crawler = Crawler_baijiahao()
    releaserUrl = 'https://haokan.baidu.com/haokan/wiseauthor?app_id=11669'
    # releaserUrl1 = 'https://haokan.hao123.com/haokan/wiseauthor?app_id=1593559247255945'
    # 看剧汪星人

    test = crawler.releaser_dynamic_page_web_by_time(releaserUrl, output_to_es_raw=True, es_index='crawler-data-raw',
                                 doc_type='doc',
                                 releaser_page_num_max=10,proxies_num=1)
    for t in test:
        print(t)
    # for i in test:
    #     print(i)
    #crawler.get_releaser_follower_num(releaserUrl)
    #crawler.video_page("","7416383329700309282")
