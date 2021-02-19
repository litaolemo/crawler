# -*- coding:utf-8 -*-
# @Time : 2019/4/17 9:15
# @Author : litao
# -*- coding: utf-8 -*-

import os
import re
import time
import copy,random
import requests
import datetime
import json
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from crawler.crawler_sys.framework.video_fields_std import Std_fields_video
from crawler.crawler_sys.utils.output_results import output_result
from crawler.crawler_sys.utils.util_logging import logged
from fontTools.ttLib import *
try:
    from crawler_sys.framework.func_get_releaser_id import *
except:
    from func_get_releaser_id import *
from crawler.crawler_sys.proxy_pool.func_get_proxy_form_kuaidaili import get_proxy
from crawler.crawler_sys.utils.func_verification_code import Login
from crawler.crawler_sys.utils.trans_str_play_count_to_int import trans_play_count

class Crawler_kwai():

    def __init__(self, timeout=None, platform='kwai'):
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
        self.first_page_headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Cache-Control": "max-age=0",
                "Connection": "keep-alive",
                "Host": "live.kuaishou.com",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36",
                # "Cookie": "did=web_c7c42d62cbb24{0}4d1ca5ffca052c3; didv=1582271776000; sid=e12d2ec74ec7af3a24d{1}cd6;pua5rv=1".format(
                #         random.randint(1000, 9000), random.randint(20, 99)),
        }
        self.loginObj = Login()
        self.get_cookies_and_front = self.loginObj.get_cookies_and_front


    def get_cookies_and_font(self,releaserUrl):
        self.cookie_dic, self.uni_code_dic = self.get_cookies_and_front(releaserUrl)
        # self.cookie_dic = {}
        # chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument('--headless')
        # chrome_options.add_argument('--disable-gpu')
        # #      driver = webdriver.Remote(command_executor='http://192.168.18.11:4444/wd/hub',
        # # desired_capabilities=DesiredCapabilities.CHROME)
        # driver = webdriver.Chrome(r'chromedriver', options=chrome_options)
        # driver.get(url)
        # time.sleep(2)
        # driver.get(url)
        # cookie = driver.get_cookies()
        # for k in cookie:
        #     self.cookie_dic[k["name"]] = k["value"]
        # # print(self.cookie_dic)
        #
        # font_face = driver.find_element_by_xpath("/html/head/style[1]")
        # font_woff_link = re.findall("url\('(.*?)'\)\s+format\('woff'\)", font_face.get_attribute("innerHTML"))
        # woff_name = font_woff_link[0].split("/")[-1]
        # print(woff_name)
        # woff = requests.get(font_woff_link[0]).content
        # os_path = "/home/hanye/"
        # this_path = os.path.isdir(os_path)
        # if not this_path:
        #     os_path = "."
        # try:
        #     f = open("%s/%s.xml" % (os_path, woff_name), encoding="utf-8")
        # except:
        #     woff = requests.get(font_woff_link[0],
        #                         headers={
        #                                 "Referer": url,
        #                                 "Sec-Fetch-Mode": "cors",
        #                                 "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36"}).content
        #     with open("%s/%s" % (os_path, woff_name), "wb") as f:
        #         f.write(woff)
        #     font = TTFont("%s/%s" % (os_path, woff_name))
        #     font.saveXML("%s/%s.xml" % (os_path, woff_name))
        #     f = open("%s/%s.xml" % (os_path, woff_name), encoding="utf-8")
        # #f = open("./%s.xml" % woff_name, encoding="utf-8")
        # self.xml_text = f.read()
        # driver.quit()
        # self.uni_code_dic = self.get_num_dic()

    def get_releaser_id(self,releaserUrl):
        return get_releaser_id(platform=self.platform, releaserUrl=releaserUrl)

    @staticmethod
    def re_cal_count(count_num):
        if isinstance(count_num, int):
            return count_num
        if isinstance(count_num, str):
            if count_num[-1] == "w":
                return int(float(count_num[:-1]) * 10000)
            try:
                return int(count_num)
            except:
                return False
        return False


    # def get_num_dic(self):
    #     xml_re = {
    #             '<TTGlyph name="(.*)" xMin="32" yMin="-6" xMax="526" yMax="729">': 0,
    #             '<TTGlyph name="(.*)" xMin="32" yMin="7" xMax="526" yMax="742">': 0,
    #             '<TTGlyph name="(.*)" xMin="98" yMin="13" xMax="363" yMax="726">': 1,
    #             '<TTGlyph name="(.*)" xMin="98" yMin="26" xMax="363" yMax="739">': 1,
    #             '<TTGlyph name="(.*)" xMin="32" yMin="13" xMax="527" yMax="732">': 2,
    #             '<TTGlyph name="(.*)" xMin="32" yMin="26" xMax="527" yMax="745">': 2,
    #             '<TTGlyph name="(.*)" xMin="25" yMin="-6" xMax="525" yMax="730">': 3,
    #             '<TTGlyph name="(.*)" xMin="25" yMin="7" xMax="525" yMax="743">': 3,
    #             '<TTGlyph name="(.*)" xMin="26" yMin="13" xMax="536" yMax="731">': 4,
    #             '<TTGlyph name="(.*)" xMin="26" yMin="26" xMax="536" yMax="744">': 4,
    #             '<TTGlyph name="(.*)" xMin="33" yMin="-5" xMax="526" yMax="717">': 5,
    #             '<TTGlyph name="(.*)" xMin="33" yMin="8" xMax="526" yMax="730">': 5,
    #             '<TTGlyph name="(.*)" xMin="39" yMin="-5" xMax="530" yMax="732">': 6,
    #             '<TTGlyph name="(.*)" xMin="39" yMin="8" xMax="530" yMax="745">': 6,
    #             '<TTGlyph name="(.*)" xMin="38" yMin="13" xMax="536" yMax="717">': 7,
    #             '<TTGlyph name="(.*)" xMin="38" yMin="26" xMax="536" yMax="730">': 7,
    #             '<TTGlyph name="(.*)" xMin="33" yMin="-7" xMax="525" yMax="731">': 8,
    #             '<TTGlyph name="(.*)" xMin="33" yMin="6" xMax="525" yMax="744">': 8,
    #             '<TTGlyph name="(.*)" xMin="37" yMin="-7" xMax="521" yMax="730">': 9,
    #             '<TTGlyph name="(.*)" xMin="37" yMin="6" xMax="521" yMax="743">': 9
    #     }
    #     uni_code_dic = {}
    #     try:
    #         for re_code in xml_re:
    #             code_dic = re.findall(re_code, self.xml_text)
    #             if code_dic:
    #                 uni_code_dic[code_dic[0].replace("uni", "\\\\u").lower()] = xml_re[re_code]
    #         print("uni_code_dic", uni_code_dic)
    #         return uni_code_dic
    #     except:
    #         print(self.xml_text,"error front_error")
    #         return False
    #
    def unicode_to_num(self,uni_str):
        count_num = str(uni_str.encode("unicode_escape"))[2:-1]
        #print(count_num)
        for i in self.uni_code_dic:
            if i in count_num:
                count_num = count_num.replace(i, str(self.uni_code_dic[i]))
        #print(count_num)
        return count_num

    @staticmethod
    def get_video_image(data):
        pass

    def get_web_url_cookies(self,releaserUrl):
        # firset_page = requests.get(releaserUrl, headers=self.first_page_headers)
        # cookie = firset_page.cookies
        firset_page = requests.get(releaserUrl, headers=self.first_page_headers)
        cookie = firset_page.cookies
        cookie = requests.utils.dict_from_cookiejar(cookie)
        cookie["pua5rv"] = "1"
        cookie["didv"] = "1582271776000"
        cookie["sid"] = "e12d2ec74ec7af3a24d{0}cd6".format(random.randint(10,99))
        cookie.pop("kuaishou.live.bfb1s",0)
        print(cookie)
        return cookie


    def releaser_page(self,releaserUrl,**kwargs):
        # for data in self.releaser_page_web(releaserUrl,**kwargs):
        #     yield data
        for data in self.releaser_page_pc(releaserUrl,**kwargs):
            yield data

    def releaser_page_web(self, releaserUrl,
                          output_to_file=False,
                          filepath=None,
                          releaser_page_num_max=5000,
                          output_to_es_raw=False,
                          es_index=None,
                          doc_type=None,
                          output_to_es_register=False,
                          push_to_redis=False, proxies_num=None, **kwargs):
        """
               get video info from api instead of web page html
               the most scroll page is 1000
               # """

        releaser = ""
        count = 1
        #        has_more = True
        retry_time = 0
        result_list = []
        releaser_id = self.get_releaser_id(releaserUrl)
        releaserUrl = 'https://live.kuaishou.com/profile/%s' % releaser_id
        principalId = releaser_id
        self.video_data['releaserUrl'] = releaserUrl
        pcursor = None
        headers = {

                "Accept": "application/json",
                #"Accept-Encoding": "gzip, deflate",
                "Accept-Language": "zh,zh-CN;q=0.9",
                #"Connection": "keep-alive",
                "Content-Type": "application/json; charset=UTF-8",
                "Cookie": "did=web_c7c42d62cbb24{0}4d1ca5ffca052c3; didv=1582271776000; sid=e12d2ec74ec7af3a24d{1}cd6;pua5rv=1".format(
                   random.randint(1000, 9000), random.randint(20, 99)),
                "Host": "kpfbeijing.m.chenzhongtech.com",
                "kpf": "H5",
                "kpn": "KUAISHOU",
                # "Origin": "https://v.kuaishou.com",
                "Origin": "https://kpfbeijing.m.chenzhongtech.com",
                "Referer": "https://kpfbeijing.m.chenzhongtech.com/fw/user/%s?fid=1535125322&cc=share_copylink&shareMethod=TOKEN&docId=0&kpn=KUAISHOU&subBiz=PROFILE&shareId=14810686%s&docABKey=share_textid_profile&shareToken=X-7AeJHKdHOc_-392ps0aWP381Bs&shareResourceType=PROFILE_OTHER&groupABKey=share_group_profile&groupName=&expTag=null&shareObjectId=916251992&shareUrlOpened=0" % (
                releaser_id, random.randint(1000, 9800)),
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Mobile Safari/537.36",

        }
        # cookies = self.get_web_url_cookies(headers["Referer"])

        proxies = get_proxy(proxies_num)
        while count <= releaser_page_num_max and count <= 1000 and pcursor != "no_more":
            try:
                if proxies_num:
                    get_page = requests.post("https://kpfbeijing.m.chenzhongtech.com/rest/kd/feed/profile",
                                             json={"eid": releaser_id, "count": 100, "pcursor": pcursor},
                                             headers=headers, timeout=10, proxies=proxies)
                else:
                    get_page = requests.post("https://kpfbeijing.m.chenzhongtech.com/rest/kd/feed/profile",
                                             json={"eid": releaser_id, "count": 100, "pcursor": pcursor},
                                             headers=headers, timeout=10)
            except:
                proxies = get_proxy(proxies_num)
                continue
            # print(get_page.content)
            time.sleep(random.randint(3,4))
            page_dic = get_page.json()
            data_list = page_dic.get("feeds")
            # print(data_list)
            # if not data_list:
            #     get_page = requests.post("https://kpfbeijing.m.chenzhongtech.com/rest/kd/feed/profile",
            #                              json={"eid": releaser_id, "count":50, "pcursor": pcursor},
            #                              headers=headers, timeout=10)
            #     page_dic = get_page.json()
            #     data_list = page_dic.get("feeds")
            #     time.sleep(2)

            if not data_list:
                print("no more data at releaser: %s page: %s " % (releaser_id, count))
                proxies = get_proxy(proxies_num)
                retry_time += 1
                if retry_time > 3:
                    proxies_num = 0
                if retry_time > 5:
                    pcursor = "no_more"
                continue
            else:
                pcursor = page_dic.get("pcursor")
                print("get data at releaser: %s page: %s" % (releaser_id, count))
                count += 1
                for info_dic in data_list:
                    video_dic = copy.deepcopy(self.video_data)
                    try:
                        video_dic['title'] = info_dic.get('caption')
                        releaser_id_ = info_dic.get("userEid")
                        photoId_list = info_dic.get('share_info').split("&")
                        for photoid in photoId_list:
                            if "photoId=" in photoid:
                                photoid = photoid.replace("photoId=", "")
                                break
                        video_dic['video_id'] = photoid
                        video_dic['url'] = "https://live.kuaishou.com/u/%s/%s" % (releaser_id_, photoid)
                        video_dic['release_time'] = info_dic.get('timestamp')
                        video_dic['releaser'] = info_dic.get("userName")
                        video_dic['play_count'] = trans_play_count(info_dic.get("viewCount"))
                        video_dic['comment_count'] = trans_play_count(info_dic.get("commentCount"))
                        video_dic['favorite_count'] = trans_play_count(info_dic.get('likeCount'))
                        video_dic['repost_count'] = trans_play_count(info_dic.get('forwardCount'))
                        video_dic['fetch_time'] = int(time.time() * 1e3)
                        try:
                            video_dic['duration'] = int(info_dic.get("ext_params").get("video")/1000)
                        except:
                            video_dic['duration'] = 0
                            print("duration error")
                        video_dic['releaser_id_str'] = "kwai_%s" % (releaser_id_)
                        video_dic['releaserUrl'] = 'https://live.kuaishou.com/profile/%s' % releaser_id_
                        video_dic['video_img'] = info_dic.get("coverUrls")[0].get("url")
                    except Exception as e:
                        print(e)
                        continue
                    if video_dic['play_count'] is False or video_dic['comment_count'] is False or video_dic[
                        'favorite_count'] is False:
                        print(info_dic)
                        continue
                    else:
                        yield video_dic
    #    @logged
    def releaser_page_pc(self, releaserUrl,
                      output_to_file=False,
                      filepath=None,
                      releaser_page_num_max=10000,
                      output_to_es_raw=False,
                      es_index=None,
                      doc_type=None,
                      output_to_es_register=False,
                      push_to_redis=False,proxies_num=None):

        """
        get video info from api instead of web page html
        the most scroll page is 1000
        """
        releaser = ""
        user_id = "153512{0}".format(random.randint(1000,9000))
        proxies = get_proxy(proxies_num)
        headers = {
            "accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Connection": "keep-alive",
            "content-type": "application/json",
            #"Cookie": "did=web_f6e24105905d4b0381d36220ad9ccda0 ;userId=%s" % (user_id),
            "Cookie": "client_key=65890b29; clientid=3; did=web_f6e24105905d4b0381d36220ad9ccda0; Hm_lvt_86a27b7db2c5c0ae37fee4a8a35033ee=1574822912; didv=1583802670821; userId=1535125321; kuaishou.live.bfb1s=477cb0011daca84b36b3a4676857e5a1; userId=1535125321; kuaishou.live.web_st=ChRrdWFpc2hvdS5saXZlLndlYi5zdBKgAfPHcR6LVRx3FRHYIe2X1-gdxEI8d1iJJnM7rTZaKtVo-54m5Bolw__9dpYJoPwvA5I2Qw_7Dgl3_8N_jicpbkpT__u6ZIxcSGC3hWmVXGufsv7zVvUALqMLknpSPVoGXlt8GFBIh4LVeEsST-ghGGWB5gpAEkU2nxVB2pXUREuQ6PEh9cc_bjoODqzcROsKFGyAYVg81qp9tnJesa1oODUaEk2hY_LIikBot7IUVtJ3ydB6KCIgUeaa89k7DGhBoXcPwlWtSUp4VbGECgvvOeIaTNFMoScoBTAB; kuaishou.live.web_ph=c41f68048b583530bfa89ab7150b24df445c",
            "Host": "live.kuaishou.com",
            "Origin": "https://live.kuaishou.com",
            "Referer": releaserUrl,
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36"
        }

        count = 1
        #        has_more = True
        retry_time = 0
        result_list = []
        releaser_id = self.get_releaser_id(releaserUrl)
        pcursor = ""
        principalId = releaser_id
        self.video_data['releaserUrl'] = releaserUrl
        while count <= releaser_page_num_max and count <= 1000 and pcursor != "no_more":
            time.sleep(random.randint(1,2))
            # self.get_cookies_and_font(releaserUrl)
            url_dic = {"operationName":"publicFeedsQuery","variables":
                {"principalId":releaser_id,"pcursor":pcursor,"count":100},
                       "query":"query publicFeedsQuery($principalId: String, $pcursor: String, $count: Int) {\n  publicFeeds(principalId: $principalId, pcursor: $pcursor, count: $count) {\n    pcursor\n    live {\n      user {\n        id\n        avatar\n        name\n        __typename\n      }\n      watchingCount\n      poster\n      coverUrl\n      caption\n      id\n      playUrls {\n        quality\n        url\n        __typename\n      }\n      quality\n      gameInfo {\n        category\n        name\n        pubgSurvival\n        type\n        kingHero\n        __typename\n      }\n      hasRedPack\n      liveGuess\n      expTag\n      __typename\n    }\n    list {\n      id\n      thumbnailUrl\n      poster\n      workType\n      type\n      useVideoPlayer\n      imgUrls\n      imgSizes\n      magicFace\n      musicName\n      caption\n      location\n      liked\n      onlyFollowerCanComment\n      relativeHeight\n      timestamp\n      width\n      height\n      counts {\n        displayView\n        displayLike\n        displayComment\n        __typename\n      }\n      user {\n        id\n        eid\n        name\n        avatar\n        __typename\n      }\n      expTag\n      __typename\n    }\n    __typename\n  }\n}\n"}
            api_url = 'https://live.kuaishou.com/m_graphql'
            try:
                if proxies:
                    get_page = requests.post(api_url, headers=headers, json=url_dic,timeout=5,proxies=proxies)
                else:
                    get_page = requests.post(api_url, headers=headers, json=url_dic,timeout=5)
            except:
                proxies = get_proxy(proxies_num)
                continue
            #print(get_page.content)
            page_dic = get_page.json()
            data_list = page_dic.get("data").get("publicFeeds").get("list")
            #print(data_list)
            if data_list == []:
                print("no more data at releaser: %s page: %s " % (releaser_id, count))
                # self.loginObj.delete_cookies(self.cookie_dic)
                proxies = get_proxy(proxies_num)
                retry_time += 1
                if retry_time > 3:
                    pcursor = "no_more"
                continue
            else:
                pcursor = page_dic.get("data").get("publicFeeds").get("pcursor")
                print("get data at releaser: %s page: %s" % (releaser_id, count))
                count += 1
                for info_dic in data_list:
                    video_dic = copy.deepcopy(self.video_data)
                    video_dic['title'] = info_dic.get('caption')
                    releaser_id = info_dic.get('user').get("eid")
                    video_dic['url'] = "https://live.kuaishou.com/u/%s/%s" % (releaser_id, info_dic.get('id'))
                    video_dic['releaser'] = info_dic.get('user').get("name")
                    video_dic['release_time'] = info_dic.get('timestamp')
                    video_dic['play_count'] = trans_play_count(info_dic.get('counts').get("displayView"))
                    video_dic['comment_count'] = trans_play_count(info_dic.get('counts').get("displayComment"))
                    video_dic['favorite_count'] = trans_play_count(info_dic.get('counts').get("displayLike"))
                    video_dic['video_id'] = info_dic.get('id')
                    video_dic['fetch_time'] = int(time.time() * 1e3)
                    video_dic['releaser_id_str'] = "kwai_%s"% (releaser_id)
                    video_dic['releaserUrl'] = 'https://live.kuaishou.com/profile/%s' % releaser_id
                    video_dic['video_img'] = self.get_video_image(info_dic)
                    if video_dic['play_count'] is False or video_dic['comment_count'] is False or video_dic['favorite_count'] is False:
                        print(info_dic)
                        continue
                    else:
                        yield video_dic

    def releaser_page_by_time(self, start_time, end_time, url,**kwargs):
        data_lis = []
        count_false = 0
        output_to_file = kwargs.get("output_to_file")
        filepath = kwargs.get("filepath")
        push_to_redis = kwargs.get("push_to_redis")
        output_to_es_register = kwargs.get("output_to_es_register")
        output_to_es_raw = kwargs.get("output_to_es_raw")
        es_index = kwargs.get("es_index")
        doc_type = kwargs.get("doc_type")
        for res in self.releaser_page(url,proxies_num=kwargs.get("proxies_num")):
            video_time = res["release_time"]
            # print(res)
            if video_time:
                if start_time < video_time:
                    if video_time < end_time:
                        data_lis.append(res)
                        if len(data_lis) >= 100:
                            output_result(result_Lst=data_lis,
                                          platform=self.platform,
                                          output_to_file=output_to_file,
                                          filepath=filepath,
                                          push_to_redis=push_to_redis,
                                          output_to_es_register=output_to_es_register,
                                          output_to_es_raw=output_to_es_raw,
                                          es_index=es_index,
                                          doc_type=doc_type)
                            data_lis.clear()

                else:
                    count_false += 1
                    if count_false > 5:
                        break
                    else:
                        continue

        if data_lis != []:
            output_result(result_Lst=data_lis,
                          platform=self.platform,
                          output_to_file=output_to_file,
                          filepath=filepath,
                          push_to_redis=push_to_redis,
                          output_to_es_register=output_to_es_register,
                          output_to_es_raw=output_to_es_raw,
                          es_index=es_index,
                          doc_type=doc_type)

    def get_releaser_follower_num(self, releaserUrl):
        count_true = 0
        headers = {
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "content-type": "application/json",
                "Referer": releaserUrl,
                "Origin": "https://live.kuaishou.com",
                "Cache-Control": "max-age=0",
                "Connection": "keep-alive",
                "Host": "live.kuaishou.com",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
        }
        while count_true < 5:
            proxies = get_proxy(proxies_num=1)
            self.get_cookies_and_font(releaserUrl)
            releaser_id = self.get_releaser_id(releaserUrl)
            if not releaser_id:
                return None, None
            post_url = 'https://live.kuaishou.com/graphql'
            post_dic = {"operationName": "userInfoQuery", "variables": {"principalId": releaser_id},
                        "query": "query userInfoQuery($principalId: String) {\n  userInfo(principalId: $principalId) {\n    id\n    principalId\n    kwaiId\n    eid\n    userId\n    profile\n    name\n    description\n    sex\n    constellation\n    cityName\n    living\n    watchingCount\n    isNew\n    privacy\n    feeds {\n      eid\n      photoId\n      thumbnailUrl\n      timestamp\n      __typename\n    }\n    verifiedStatus {\n      verified\n      description\n      type\n      new\n      __typename\n    }\n    countsInfo {\n      fan\n      follow\n      photo\n      liked\n      open\n      playback\n      private\n      __typename\n    }\n    bannedStatus {\n      banned\n      defriend\n      isolate\n      socialBanned\n      __typename\n    }\n    __typename\n  }\n}\n"}
            try:
                releaser_page = requests.post(post_url, headers=headers, cookies=self.cookie_dic, json=post_dic,
                                              proxies=proxies, timeout=2)
            except:
                releaser_page = requests.post(post_url, headers=headers, cookies=self.cookie_dic,
                                              json=post_dic)
            res_dic = releaser_page.json()
            print(res_dic)
            if res_dic.get("errors"):
                self.loginObj.delete_cookies(self.cookie_dic)
            try:
                releaser_follower_num_str = res_dic["data"]["userInfo"]["countsInfo"]["fan"]
                releaser_follower_num = self.re_cal_count(self.unicode_to_num(releaser_follower_num_str))
                print(releaser_follower_num)
                releaser_img = self.get_releaser_image(data=res_dic)
                return releaser_follower_num, releaser_img
            except:
                if count_true == 4:
                    self.loginObj.delete_cookies(self.cookie_dic)
                count_true += 1
        return None, None

    # @staticmethod
    # def get_video_image(data):
    #     return data.get("poster")

    # def get_releaser_follower_num(self, releaserUrl):
    #     self.get_cookies_and_font(releaserUrl)
    #     releaser_id = self.get_releaser_id(releaserUrl)
    #     releaserUrl = 'https://live.kuaishou.com/graphql'
    #     post_dic = {"operationName":"userInfoQuery","variables":{"principalId":releaser_id},"query":"query userInfoQuery($principalId: String) {\n  userInfo(principalId: $principalId) {\n    id\n    principalId\n    kwaiId\n    eid\n    userId\n    profile\n    name\n    description\n    sex\n    constellation\n    cityName\n    living\n    watchingCount\n    isNew\n    privacy\n    feeds {\n      eid\n      photoId\n      thumbnailUrl\n      timestamp\n      __typename\n    }\n    verifiedStatus {\n      verified\n      description\n      type\n      new\n      __typename\n    }\n    countsInfo {\n      fan\n      follow\n      photo\n      liked\n      open\n      playback\n      private\n      __typename\n    }\n    bannedStatus {\n      banned\n      defriend\n      isolate\n      socialBanned\n      __typename\n    }\n    __typename\n  }\n}\n"}
    #     releaser_page = requests.post(releaserUrl, headers=self.first_page_headers, cookies=self.cookie_dic,json=post_dic)
    #     res_dic = releaser_page.json()
    #
    #     try:
    #         releaser_follower_num_str =res_dic["data"]["userInfo"]["countsInfo"]["fan"]
    #         releaser_follower_num = self.re_cal_count(self.unicode_to_num(releaser_follower_num_str))
    #         print(releaser_follower_num)
    #         releaser_img = self.get_releaser_image(data=res_dic)
    #         return releaser_follower_num,releaser_img
    #     except:
    #         return None

    def get_releaser_image(self, releaserUrl=None,data=None):
        if releaserUrl:
            self.get_cookies_and_font(releaserUrl)
            releaser_id = self.get_releaser_id(releaserUrl)
            releaserUrl = 'https://live.kuaishou.com/graphql'
            post_dic = {"operationName": "userInfoQuery", "variables": {"principalId": releaser_id},
                        "query": "query userInfoQuery($principalId: String) {\n  userInfo(principalId: $principalId) {\n    id\n    principalId\n    kwaiId\n    eid\n    userId\n    profile\n    name\n    description\n    sex\n    constellation\n    cityName\n    living\n    watchingCount\n    isNew\n    privacy\n    feeds {\n      eid\n      photoId\n      thumbnailUrl\n      timestamp\n      __typename\n    }\n    verifiedStatus {\n      verified\n      description\n      type\n      new\n      __typename\n    }\n    countsInfo {\n      fan\n      follow\n      photo\n      liked\n      open\n      playback\n      private\n      __typename\n    }\n    bannedStatus {\n      banned\n      defriend\n      isolate\n      socialBanned\n      __typename\n    }\n    __typename\n  }\n}\n"}
            releaser_page = requests.post(releaserUrl, headers=self.first_page_headers, cookies=self.cookie_dic,
                                          json=post_dic)
            res_dic = releaser_page.json()
            try:
                releaser_img = res_dic["data"]["userInfo"]["profile"]
                print(releaser_img)
                return releaser_img
            except:
                return None
        else:
            releaser_img = data["data"]["userInfo"]["profile"]
            print(releaser_img)
            return releaser_img

if __name__ == '__main__':
    test = Crawler_kwai()
    url = 'https://live.kuaishou.com/profile/IIloveyoubaby'
    user_lis = [
           "https://live.kuaishou.com/profile/3xx3vac2uctn2ak",
            "https://live.kuaishou.com/profile/3xn3bwab5q5pehc",
            "https://live.kuaishou.com/profile/3x25rpyd2v6qv86",
            "https://live.kuaishou.com/profile/3xziqtumtj6xzkm",
            "https://live.kuaishou.com/profile/3xugxze3x97r2w4",
            "https://live.kuaishou.com/profile/3xnhsx3wt8if74c",
            "https://live.kuaishou.com/profile/3xcdy8gzaqsy6zu",
            "https://live.kuaishou.com/profile/3xid2nndq4fk3t6",
            "https://live.kuaishou.com/profile/3xeqdvqjp553nvm",
            "https://live.kuaishou.com/profile/3xhyd98hf8bcxsi",
            "https://live.kuaishou.com/profile/3x9m4am579hrvfq",
            "https://live.kuaishou.com/profile/3xkkkfxxd48dei6",
            "https://live.kuaishou.com/profile/3xjqv4mspa4bw8i",
            "https://live.kuaishou.com/profile/3xgxy8yiqnqtkt4",
            "https://live.kuaishou.com/profile/3xxxsfnuie3ucfi",
            "https://live.kuaishou.com/profile/3x3t5epr3max95y",
            "https://live.kuaishou.com/profile/3xny3xcwghpi7yk",
            "https://live.kuaishou.com/profile/3xcm5jbmtyhezj4",
            "https://live.kuaishou.com/profile/3x7twx645iyapkm",
            "https://live.kuaishou.com/profile/3xzu7cr583vt5zg",
            "https://live.kuaishou.com/profile/3x83h23sbmjxiq4",
            "https://live.kuaishou.com/profile/3xijrqj69f8ckjk",
            "https://live.kuaishou.com/profile/3xv47uc6r7gq882",
            "https://live.kuaishou.com/profile/3xqv5w2kzccqk5y",
            "https://live.kuaishou.com/profile/3x5yh4d4wtfnvs6",
            "https://live.kuaishou.com/profile/3xwxakqi8whe9ju",
            "https://live.kuaishou.com/profile/3x58zdtbg9ecqxy",
            "https://live.kuaishou.com/profile/3xd6szsxerggfu4",
            "https://live.kuaishou.com/profile/3xzfhbh9q2afynk",
            "https://live.kuaishou.com/profile/3xgy42jat4kp8yk",
            "https://live.kuaishou.com/profile/3xq9ya2akj52f8a",
            "https://live.kuaishou.com/profile/3xvbgjz6vxdr4zq",
            "https://live.kuaishou.com/profile/3xaqiypqnwfwakg",
            "https://live.kuaishou.com/profile/3x57bdie5v8bag6",
            "https://live.kuaishou.com/profile/3x6e8hdhmtssdae",
            "https://live.kuaishou.com/profile/3xeszmbm3rphnn4",
            "https://live.kuaishou.com/profile/3x67fxqg6wd39tw",

    ]
    for u in user_lis:
        ttt = test.releaser_page_by_time(1587225600000,1587965489795   ,u, output_to_es_raw=True,
                                 es_index='crawler-data-raw',
                                 doc_type='doc',
                                 releaser_page_num_max=2000,proxies_num=8)
    #     break
