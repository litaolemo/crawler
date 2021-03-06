# -*- coding:utf-8 -*-
# @Time : 2020/1/8 17:31 
# @Author : litao
import os
import re
import time
import copy
import requests
import datetime
import json, random, urllib
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from crawler.crawler_sys.framework.video_fields_std import Std_fields_video
from crawler.crawler_sys.utils.output_results import output_result
from crawler.crawler_sys.utils.util_logging import logged
from crawler.crawler_sys.proxy_pool.func_get_proxy_form_kuaidaili import get_proxy

try:
    from crawler_sys.framework.func_get_releaser_id import *
except:
    from func_get_releaser_id import *
from crawler.crawler_sys.utils.output_results import retry_get_url


class Crawler_douyin():

    def __init__(self, timeout=None, platform='抖音'):
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
            self.headers = {
                    "Accept-Encoding": "gzip",
                    # "X-SS-REQ-TICKET": "1589357171319",
                    "X-SS-REQ-TICKET": str(int(datetime.datetime.now().timestamp() * 1e3)),
                    "sdk-version": "1",
                    "User-Agent": "ttnet okhttp/3.10.0.2",
                    # "Cookie": "odin_tt=a079fbd0c726109f9f513d911e5869bed7b45822e99af630b65dbe6a889561095770f8357f6ce1a69c10ced468695be448e75eeaae4f9fd4cae68b90db6d661d; install_id=1697284012668695; ttreq=1$31eace644c19346ed8397afb3953495afac05b2b",
                    "X-Gorgon": "0401e0ce4001b09c16b91c4741bd4eb2ca69dfd4d031374a8e72",
                    # "X-Khronos": "1589357171",
                    "X-Khronos": str(int(datetime.datetime.now().timestamp())),
                    "Host": "aweme.snssdk.com",
                    "Connection": "Keep-Alive",
            }
        # self.headers = {
        #         # 抖音极速版
        #         # "Host": "api3-normal-c-hl.amemv.com",
        #         "Connection": "keep-alive",
        #         # "X-SS-TC": "0",
        #         "User-Agent": "Android 8.1.0; zh-CN; EML-AL00 Build/HUAWEIEML-AL00",
        #         # "x-tt-trace-id": "00-0a1eeba%sb4ea508f44a29d%s-0a1ppwc%sb4ea50-01" % (random.randint(100,999),random.randint(10000000,99999999),random.randint(100,999)),
        #         "Accept-Encoding": "gzip",
        #         "X-SS-REQ-TICKET": str(int(datetime.datetime.now().timestamp() * 1e3)),
        #         # "Cookie": 'tt_webid=6636348501838333443; __tea_sdk__user_unique_id=6636348501838333443; _ga=GA1.3.400580664.1533338821; sid_guard=7055c05aa0c030d8872df921e4198d7d%7C1573351823%7C5184000%7CThu%2C+09-Jan-2020+02%3A10%3A23+GMT; uid_tt=cce96b02e4a2ad0dcc2ce9c2edad93a3; sid_tt=7055c05aa0c030d8872df921e4198d7d; sessionid=7055c05aa0c030d8872df921e4198d7d; install_id=93682014831; ttreq=1$9164e2a9cc97597197bec6e03559f7e9d05982c5; odin_tt=cc424af4c816fe1492acb2b887cef5fae585799744a9168f8204fce3d0f011e4694ff93a6a6f3e83bc3e126418aafa54',
        #         "X-Khronos": str(int(datetime.datetime.now().timestamp())),
        #         # "sdk-version": "1",
        #         # "X-Gorgon": "83009990000046140d8188c11cfdc1dd7b3f0507077b39112481",
        #
        # }
        self.api_list = [
                "api3-normal-c-hl.amemv.com",
                "api3-normal-c-lf.amemv.com",
                "api3-normal-c-lq.amemv.com",
                "aweme.snssdk.com",
        ]

    def get_releaser_id(self, releaserUrl):
        return get_releaser_id(platform=self.platform, releaserUrl=releaserUrl)

    def get_releaser_follower_num(self, releaserUrl):
        releaser_id = self.get_releaser_id(releaserUrl)
        releaserUrl = 'https://{2}/aweme/v1/user/?ac=WIFI&device_id={1}&os_api=18&app_name=aweme&channel=App Store&device_platform=ipad&device_type=iPad6,11&app_version=8.7.1&js_sdk_version=1.17.2.0&version_code=8.7.1&os_version=13.2.3&screen_width=1536&user_id={0}'.format(
                releaser_id, str(random.randint(40000000000, 90000000000)), random.choice(self.api_list))
        count = 0
        while count < 3:
            try:
                count += 1
                time.sleep(random.randint(1, 2))
                get_page = retry_get_url(releaserUrl, headers=self.headers, proxies=1)
                page = get_page.json()
                follower_num = page["user"].get("follower_count")
                print('%s follower number is %s' % (releaserUrl, follower_num))
                releaser_img = page["user"].get("avatar_thumb").get("url_list")[0]
                return follower_num, releaser_img
            except:
                print("can't find followers")
                continue
        else:
            return None, None

    def find_releaser_id(self, releaserUrl):
        return get_releaser_id(platform=self.platform, releaserUrl=releaserUrl)

    def releaser_page(self, releaserUrl,
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
        """

        result_list = []
        has_more = True
        count = 1
        count_false = 0
        releaser_id = self.find_releaser_id(releaserUrl)
        offset = "0"
        # vid = "AB5483CA-FCDC-42F1-AFB1-077A1%sDA" % random.randint(100000, 999999)
        # ccid = "F153594D-1310-4984-A4C3-A679D4D%s" % random.randint(10000, 99999)
        # openudid = "5d44f2ea1b74e3731b27e5ed8039ac29f%s" % random.randint(1000000, 9999999)
        # idfa = "E3FC9054-384B-485F-9B4C-936F33D7D%s" % random.randint(100, 999)
        # iid = str(random.randint(40000000000, 70000000000))
        device_id = str(random.randint(56884000000, 96890000000))
        # proxies = get_proxy(proxies_num)
        while has_more and count <= releaser_page_num_max:
            # print(str(releaser_id)+str(max_behot_time))
            # js_head = json.loads(get_js(str(releaser_id)+str(max_behot_time)))
            time.sleep(random.randint(1,2))
            print("get %s video on page %s" % (releaser_id, count))
            # url_dic = {
            #         "source": "0",
            #         "max_cursor": offset,
            #         "user_id": releaser_id,
            #         "count": "21",
            #         "os_api": "23",
            #         "device_type": "Huawei P20",
            #         "ssmix": "a",
            #         "manifest_version_code": "985",
            #         "dpi": "429",
            #         "uuid": "440000000189785",
            #         "app_name": "douyin",
            #         "version_name": "9.8.5",
            #         # "ts": "1585532172",
            #         "app_type": "normal",
            #         "ac": "wifi",
            #         "update_version_code": "9852",
            #         "channel": "baidu",
            #         # "_rticket": "1585532172572",
            #         "device_platform": "android",
            #         # "iid": "109688778422",
            #         "version_code": "985",
            #         # "cdid": "87cc1c77-cc3c-41a1-8df6-1e060b9c510b",
            #         # "openudid": "e44cc0264b92bcbf",
            #         "device_id": device_id,
            #         "resolution": "1080*2244",
            #         "os_version": "9.0.1",
            #         "language": "zh",
            #         "device_brand": "Huawei",
            #         "aid": "2329",
            #         "mcc_mnc": "46005",
            # }

            # url_dic = {
            #         "ac": "WIFI",
            #         # "iid": iid,
            #         "device_id": device_id,
            #         "os_api": "18",
            #         "app_name": "aweme",
            #         "channel": "App Store",
            #         # "idfa": "7AED33DD-0F97-418D-AFAA-72ED0578A44E",
            #         # "idfa": idfa,
            #         "device_platform": "iphone",
            #         "build_number": "92113",
            #         # "vid": "21B39A50-8C28-4E7E-AEB8-A67B12B1A82B",
            #         # "vid": vid,
            #         # "openudid": "b1021c76124449e0e9f0e43bdf51f3314aac263b",
            #         # "openudid": openudid,
            #         "device_type": "iPhone9,4",
            #         "app_version": "9.2.1",
            #         "js_sdk_version": "1.43.0.1",
            #         "version_code": "9.2.1",
            #         "os_version": "13.3",
            #         "screen_width": "1242",
            #         "aid": "2329",
            #         "mcc_mnc": "",
            #         "user_id": releaser_id,
            #         "max_cursor": offset,
            #         "count": "21",
            #         "source": "0",
            # }
            url_dic = {
                    "source": "0",
                    "max_cursor": offset,
                    "user_id": releaser_id,
                    "count": "21",
                    "os_api": "23",
                    "device_type": "Huawei P20",
                    "ssmix": "a",
                    "manifest_version_code": "10000%s" % random.randint(1,5),
                    "dpi": "429",
                    # "uuid": "440000000189785",
                    "app_name": "douyin_lite",
                    "version_name": "10.0.0",
                    # "ts": "1585532172",
                    "app_type": "normal",
                    "ac": "wifi",
                    "update_version_code": "10009900",
                    "channel": "baidu",
                    # "_rticket": "1585532172572",
                    "device_platform": "android",
                    # "iid": "1697284012668695",
                    "version_code": "100%s00" %random.randint(1,5),
                    # "cdid": "87cc1c77-cc3c-41a1-8df6-1e060b9c510b",
                    # "openudid": "e44cc0264b92bcbf",
                    "device_id": device_id,
                    # "device_id": 69418894872,
                    "resolution": "1080*2244",
                    "os_version": "9.0.1",
                    "language": "zh",
                    "device_brand": "Huawei",
                    "aid": "2329",
                    "mcc_mnc": "46001",
            }
            # self.headers["Host"] = host
            url = "https://{1}/aweme/v1/aweme/post/?{0}".format(urllib.parse.urlencode(url_dic),random.choice(self.api_list))
            try:
                #proxies = get_proxy(proxies_num)
                if proxies_num:
                    # get_page = requests.get(url, headers=self.headers, proxies=proxies, timeout=10)
                    get_page = retry_get_url(url, headers=self.headers, proxies=proxies_num, timeout=10)
                else:
                    get_page = requests.get(url, headers=self.headers, timeout=10)
            except Exception as e:
                proxies = get_proxy(proxies_num)
                print(e)
                continue

            page_dic = {}
            # print(get_page.text)
            try:
                page_dic = get_page.json()
                # print(get_page)
                # print(page_dic)
                data_list = page_dic.get('aweme_list')
                if not data_list:
                    get_page = requests.get(url, headers=self.headers, timeout=10)
                    page_dic = get_page.json()
                    data_list = page_dic.get('aweme_list')
                    if not data_list:
                        raise ValueError
                has_more = page_dic.get('has_more')
                offset = str(page_dic.get("max_cursor"))
            except:
                if not data_list:
                    proxies = get_proxy(proxies_num)
                    count_false += 1
                    if count_false >= 2:
                        break
                    else:
                        continue
            # offset = page_dic.get('offset')

            if has_more is None:
                has_more = False
            if data_list == []:
                print("no data in releaser %s page %s" % (releaser_id, count))
                # print(page_dic)
                # print(url)
                proxies = get_proxy(1)
                count_false += 1
                if count_false >= 2:
                    has_more = False
                continue

            else:
                count_false = 0
                count += 1
                for one_video in data_list:
                    # info_str = one_video.get('content')
                    video_dic = copy.deepcopy(self.video_data)
                    video_dic['title'] = one_video.get('desc')
                    video_dic['url'] = one_video.get('share_url')
                    video_dic['releaser'] = one_video.get('author').get("nickname")
                    video_dic['releaserUrl'] = releaserUrl
                    release_time = one_video.get('create_time')
                    video_dic['release_time'] = int(release_time * 1e3)
                    try:
                        video_dic['duration'] = int(one_video.get('duration') / 1000)
                    except:
                        video_dic['duration'] = 0
                    video_dic['play_count'] = 0
                    video_dic['repost_count'] = one_video.get('statistics').get('share_count')
                    video_dic['comment_count'] = one_video.get('statistics').get('comment_count')
                    video_dic['favorite_count'] = one_video.get('statistics').get('digg_count')
                    video_dic['video_id'] = one_video.get('aweme_id')
                    video_dic['fetch_time'] = int(datetime.datetime.now().timestamp() * 1e3)
                    video_dic['releaser_id_str'] = "抖音_%s" % releaser_id
                    try:
                        video_dic['video_img'] = one_video.get('video').get('cover').get('url_list')[0]
                    except:
                        pass
                    yield video_dic

    def releaser_page_by_time(self, start_time, end_time, url,allow,**kwargs):
        count_false = 0
        for res in self.releaser_page(url,proxies_num=kwargs.get("proxies_num")):
            video_time = res["release_time"]
            # print(res)
            if video_time:
                if start_time < video_time:
                    if video_time < end_time:
                        yield res
                else:
                    count_false += 1
                    if count_false > allow:
                        break
                    else:
                        yield res

# test
if __name__ == '__main__':
    test = Crawler_douyin()
    url = 'https://www.iesdouyin.com/share/user/104881369596'
    user_lis = [
            # "https://www.iesdouyin.com/share/user/108115110920",
            # "https://www.iesdouyin.com/share/user/2541669947541709",
            # "https://www.iesdouyin.com/share/user/107661537446",
            #"https://www.iesdouyin.com/share/user/73812850451",
            "https://www.iesdouyin.com/share/user/958362042508243",
            "https://www.iesdouyin.com/share/user/92786666943",
            "https://www.iesdouyin.com/share/user/945190971379859",
            "https://www.iesdouyin.com/share/user/101819301280",
            "https://www.iesdouyin.com/share/user/80961113643",
            "https://www.iesdouyin.com/share/user/83424822076",
            "https://www.iesdouyin.com/share/user/2510898623686052",
            "https://www.iesdouyin.com/share/user/923182482332285",
            "https://www.iesdouyin.com/share/user/1714849703798029",
            "https://www.iesdouyin.com/share/user/59616878617",
            "https://www.iesdouyin.com/share/user/57910335931",
            "https://www.iesdouyin.com/share/user/103351994197",
            "https://www.iesdouyin.com/share/user/103734266340",
            "https://www.iesdouyin.com/share/user/80817084456",
            "https://www.iesdouyin.com/share/user/75940122416",
            "https://www.iesdouyin.com/share/user/4160164992909407",
            "https://www.iesdouyin.com/share/user/3460888679943587",
            "https://www.iesdouyin.com/share/user/87781698767",
            "https://www.iesdouyin.com/share/user/110808885367",
            "https://www.iesdouyin.com/share/user/64451062019",
            "https://www.iesdouyin.com/share/user/65379960606",
            "https://www.iesdouyin.com/share/user/77518636875",
            "https://www.iesdouyin.com/share/user/84705992973",
            "https://www.iesdouyin.com/share/user/95420372976",
            "https://www.iesdouyin.com/share/user/111401782458",
            "https://www.iesdouyin.com/share/user/64826520759",
            "https://www.iesdouyin.com/share/user/104798575953",
            "https://www.iesdouyin.com/share/user/60429993167",
            "https://www.iesdouyin.com/share/user/106263629798",
            "https://www.iesdouyin.com/share/user/3056295650011230",
            "https://www.iesdouyin.com/share/user/96121128763",
            "https://www.iesdouyin.com/share/user/1538926068907102",
            "https://www.iesdouyin.com/share/user/51551011170",
            "https://www.iesdouyin.com/share/user/1310230364563437",
            "https://www.iesdouyin.com/share/user/93106603716",
            "https://www.iesdouyin.com/share/user/4199739197495693",
            "https://www.iesdouyin.com/share/user/106116419864",
            "https://www.iesdouyin.com/share/user/67931977592",
            "https://www.iesdouyin.com/share/user/57879628478",
            "https://www.iesdouyin.com/share/user/694501863598767",
            "https://www.iesdouyin.com/share/user/92362920712",
            "https://www.iesdouyin.com/share/user/69059745290",
            "https://www.iesdouyin.com/share/user/75455451131",
            "https://www.iesdouyin.com/share/user/4160150793618815",
            "https://www.iesdouyin.com/share/user/95759857442",
            "https://www.iesdouyin.com/share/user/62816216675",
            "https://www.iesdouyin.com/share/user/1349846462114973",
            "https://www.iesdouyin.com/share/user/166743642223588",

    ]
    for u in user_lis:
        ttt = test.releaser_page_by_time(1577808000000, 1587693957328,u, output_to_es_raw=True,
                                 es_index='crawler-data-raw',
                                 doc_type='doc',
                                 releaser_page_num_max=5, proxies_num=1,allow=20)
        for res in ttt:
            print(res)
        # test.get_releaser_follower_num(u)
        # break
