# -*- coding:utf-8 -*-
# @Time : 2020/4/20 14:02 
# @Author : litao
# -*- coding:utf-8 -*-
# @Time : 2019/5/5 9:48
# @Author : litao

import os
import re
import time
import copy
import requests
import datetime
import json
from bs4 import BeautifulSoup
from crawler.crawler_sys.framework.video_fields_std import Std_fields_video
from crawler.crawler_sys.utils.output_results import output_result
from crawler.crawler_sys.utils.util_logging import logged
import urllib
try:
    from crawler_sys.framework.func_get_releaser_id import *
except:
    from func_get_releaser_id import *
from crawler.crawler_sys.proxy_pool.func_get_proxy_form_kuaidaili import get_proxy

class Crawler_wangyi_news():

    def __init__(self, timeout=None, platform='喜马拉雅'):
        if timeout == None:
            self.timeout = 10
        else:
            self.timeout = timeout
        self.platform = platform
        self.TotalVideo_num = None
        self.midstepurl = None
        std_fields = Std_fields_video()
        self.video_data = std_fields.video_data
        self.video_data['platform'] = self.platform
        unused_key_list = ['channel', 'describe', 'repost_count', 'isOriginal']
        for key in unused_key_list:
            self.video_data.pop(key)



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

    def get_releaser_image(self,releaserUrl=None,data=None):
        if releaserUrl:
            proxies_num = get_proxy(proxies_num=1)
            releaser_id = self.get_releaser_id(releaserUrl)
            url = "http://c.m.163.com/nc/subscribe/v2/topic/%s.html" % releaser_id
            res = requests.get(url, headers=self.headers,proxies=proxies_num)
            res_json = res.json()
            try:
                releaser_img_url = res_json.get("subscribe_info").get("topic_icons")
                return releaser_img_url
            except:
                print("can't can releaser_img")
        else:
            releaser_img_url = data.get("subscribe_info").get("topic_icons")
            return releaser_img_url

    @staticmethod
    def get_video_image(data):
        try:
            video_photo_url = data["imgsrc"]
        except:
            video_photo_url = data["cover"]
        return video_photo_url


    def video_page(self, url,
                   output_to_file=False,
                   filepath=None,
                   releaser_page_num_max=30,
                   output_to_es_raw=False,
                   es_index=None,
                   doc_type=None,
                   output_to_es_register=False,
                   push_to_redis=False, *args,**kwargs):

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
            print(releaserUrl)
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
                    video_dic['release_time'] = int(info_dic.get('ptime'))
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
                    video_dic['video_img'] = self.get_video_image(info_dic)

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
                      releaser_page_num_max=30,
                      output_to_es_raw=False,
                      es_index=None,
                      doc_type=None,
                      output_to_es_register=False,
                      push_to_redis=False,proxies_num=None):

        """
        get video info from api instead of web page html
        the most scroll page is 1000
        """
        proxies = get_proxy(proxies_num)
        releaser = ""
        count = 1
        count_false = 0
        result_list = []
        releaser_id = self.get_releaser_id(releaserUrl)
        page_count = 1
        pcursor = None
        timestamp = int(time.time() * 1e3)

        while count <= releaser_page_num_max and count <= 1000 and pcursor != "no_more":
            url = "http://mobile.ximalaya.com/mobile/v1/album/track/ts-{0}?albumId={1}&device=android&isAsc=true&isQueryInvitationBrand=true&pageId={2}&pageSize=20&pre_page=0".format(timestamp,releaser_id,page_count)
            proxies = get_proxy(proxies_num)
            try:
                if proxies:
                    get_page = requests.get(releaserUrl, headers=headers, timeout=5,proxies=proxies)
                else:
                    get_page = requests.get(releaserUrl, headers=headers,timeout=5)
                # print(data_list)
                # print(releaserUrl)
                page_dic = get_page.json()
                data_list = page_dic.get("tab_list")
            except:
                proxies = get_proxy(1)
                count_false += 1
                if count_false <=5:
                    continue
                else:
                    break

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
                    video_dic['releaserUrl'] = "https://c.m.163.com/news/sub/%s.html" % releaser_id
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
                    video_dic['video_img'] = self.get_video_image(info_dic)
                    result_list.append(video_dic)
                    time.sleep(0.5)
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

    def get_releaser_follower_num(self,releaserUrl):
        proxies = get_proxy(1)
        releaser_id = self.get_releaser_id(releaserUrl)
        url = "http://c.m.163.com/nc/subscribe/v2/topic/%s.html" % releaser_id
        res = requests.get(url,headers=self.headers,proxies=proxies)
        res_json = res.json()
        try:
            follower_num = self.forllower_num_to_int(res_json.get("subscribe_info").get("subnum"))
            releaser_img_url = self.get_releaser_image(data=res_json)
            print('%s follower number is %s' % (releaserUrl, follower_num))
            return follower_num,releaser_img_url
        except:
            print("can't can followers")


if __name__ == '__main__':
    test = Crawler_wangyi_news()
    # test.video_page("")
    # url = 'https://live.kuaishou.com/profile/IIloveyoubaby'
    user_lis = ["https://c.m.163.com/news/sub/T1512044479072.html"]
    for u in user_lis:
        ttt = test.releaser_page(releaserUrl=u, output_to_es_raw=True,
                                 es_index='crawler-data-raw',
                                 doc_type='doc',
                                 releaser_page_num_max=300,proxies_num=1)
