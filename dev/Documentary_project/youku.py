# -*- coding:utf-8 -*-
# @Time : 2019/12/25 10:55 
# @Author : litao
# -*- coding:utf-8 -*-
# @Time : 2019/12/19 12:01
# @Author : litao

import numpy as np
import random
import json, redis, re, requests
from selenium.webdriver import ActionChains
import time, datetime, copy
from selenium import webdriver
from PIL import Image
import os
from selenium.webdriver.support.ui import WebDriverWait
import cv2
from fontTools.ttLib import *
from crawler.crawler_sys.utils.trans_str_play_count_to_int import trans_play_count
from crawler.crawler_sys.utils.trans_duration_str_to_second import trans_duration
from concurrent.futures import ProcessPoolExecutor
import urllib
from bs4 import BeautifulSoup

# rds_list = redis.StrictRedis(host='127.0.0.1', port=6379, db=1, decode_responses=True)
# rds_single = redis.StrictRedis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
# rds_get = redis.StrictRedis(host='127.0.0.1', port=6379, db=15, decode_responses=True)


# rds_copy = redis.StrictRedis(host='127.0.0.1', port=6379, db=1, decode_responses=True)
rds_list = redis.StrictRedis(host='192.168.17.60', port=6379, db=1, decode_responses=True)
rds_single = redis.StrictRedis(host='192.168.17.60', port=6379, db=0, decode_responses=True)
rds_get = redis.StrictRedis(host='192.168.17.60', port=6379, db=15, decode_responses=True)


def revise_data():
    scan_re = rds_single.scan_iter()
    for one_scan in scan_re:
        # print(one_scan)
        data = rds_single.hgetall(one_scan)
        # data["title"] = data["title"].replace("\r", "").replace("\n", "")
        # data["describe"] = data["describe"].replace("\r", "").replace("\n", "")
        rds_get.hmset(one_scan, data)
        # rds_list.hmset(one_scan,data)


class CrawlerMain(object):
    def __init__(self):
        self.chrome_options = webdriver.ChromeOptions()
        # self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--disable-gpu')
        # self.chrome_options.add_argument("--start-maximized")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        self.timestamp = str(datetime.datetime.now().timestamp() * 1e3)
        prefs = {"profile.managed_default_content_settings.images": 2}
        self.chrome_options.add_experimental_option("prefs", prefs)
        # self.driver = webdriver.Chrome(options=self.chrome_options)
        self.one_video_dic = {
                "platform": "youku_video",
                "ID": "",
                "title": "",
                "url": "",
                "describe": "",
                "video_count": "",
                "sum_duration": "",
                "year": "",
                "provider": "",
                "style_tags": "",
                "project_tags": "",
                "language": "",
                "area": "",
                "if_pay": "",
                "play_count_sum": "",
                "play_heat": "",
                "rate": "",
                "favorite_count_sum": "",
                "comment_count_sum": "",
                "barrage_count": "",
        }
        self.single_video_dic = {
                "platform": "youku_video",
                "vid": "",
                "title": "",
                "video_title": "",
                "url": "",
                "video_url": "",
                "describe": "",
                "video_count": "",
                "duration": "",
                "year": "",
                "provider": "",
                "language": "",
                "area": "",
                "if_pay": "",
                "play_count": "",
                "play_heat": "",
                "rate": "",
                "favorite_count": "",
                "comment_count": "",
                "barrage_count": "",
        }

    def __exit__(self):
        self.driver.close()

    def list_page(self, releaserUrl=None,
                  video_list_xpath=None,
                  play_count_xpath=None,
                  if_pay="",
                  title=None,
                  describe=None,
                  project_tag_xpath=None,
                  provider="",
                  year=None,
                  next_page_xpath=None,
                  roll=None,
                  project_tag=None,
                  style_tags="",
                  category_id="",
                  area="",

                  ):
        page = 1
        while True:
            headers = {
                    "accept": "text/javascript, text/html, application/xml, text/xml, */*",
                    "accept-encoding": "gzip, deflate, br",
                    "accept-language": "zh,zh-CN;q=0.9",
                    "content-type": "application/x-www-form-urlencoded",
                    "cookie": "__ysuid=1553755022290AhL; juid=01d9bjgm0l2ngc; cna=2U8aFb1yaVcCAdr3nSX3f47K; __aryft=1557994090; __artft=1557994090; UM_distinctid=16eea2ee55739c-08e02b1a26b73c-2393f61-161012-16eea2ee55881b; yseidcount=3; ysestep=4; ystep=9; ykss=f0cdf05d77e5a6dcebeb4c1c; __ayft=1576549468599; _zpdtk=6af412f14f2c8cf070f92fb13c6ae2520f377b5e; __arycid=dz-3-00; __arcms=dz-3-00; seid=01dse2qnuq4hm; referhost=http%3A%2F%2Fv.qq.com; seidtimeout=1576727533339; CNZZDATA1277956656=459872710-1575883398-%7C1577236849; __aysid=1577241653062aoh; __arpvid=1577241653062y0Cvnk-1577241653157; __ayscnt=5; __aypstp=63; __ayspstp=2; P_ck_ctl=8631B68A252D2554586670E8AF58DC31; isg=BPn5lcr2g-qL6125hb2akn4pCGUTrtPScjDNIxsvSyAeohg0YlDyiX-8IO7xGoXw",
                    "referer": "https://list.youku.com/category/show/c_84.html?spm=a2ha1.12701310.app.5~5!2~5!2~5~5~DL~DD~A!8",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36",
                    "x-requested-with": "XMLHttpRequest"
            }
            quary = {
                    "c": "84",
                    "type": "show",
                    "p": str(page),
                    "a": area,
                    "g": style_tags,
                    "pr": provider,
                    "pt": if_pay
            }

            if page >= 1:
                requests_url = "https://list.youku.com/category/page?%s" % urllib.parse.urlencode(quary)
                res = requests.get(requests_url, headers=headers)
                print("get data at page %s" % page)
                page += 1
            else:
                break
            res_json = res.json()
            print(res_json)
            vidoe_list_obj = res_json["data"]
            if not vidoe_list_obj:
                break
            try:
                for count, one_video in enumerate(vidoe_list_obj):
                    print(count)
                    id = one_video["videoId"]
                    title = one_video["title"]
                    project_name = "youku_%s" % id
                    url = one_video["videoLink"]
                    describe = one_video["subTitle"]
                    video_count = one_video["summary"]
                    access = one_video["access"]
                    data_dic = {
                            "id": id,
                            "url": url,
                            "title": title,
                            "video_count": video_count,
                            "access": access,
                            "describe": describe,

                    }
                    if style_tags:
                        temp_dic = {}
                        try:
                            temp_dic["style_tags"] = temp_dic["style_tags"] + style_tags
                        except:
                            temp_dic["style_tags"] = style_tags
                        data_dic.update(temp_dic)
                    if project_tag:
                        temp_dic = {}
                        temp_dic["project_tags"] = project_tag
                        data_dic.update(temp_dic)
                    if area:
                        temp_dic = {}
                        temp_dic["area"] = area
                        data_dic.update(temp_dic)
                    if year:
                        temp_dic = {}
                        temp_dic["year"] = year
                        data_dic.update(temp_dic)
                    if provider:
                        temp_dic = {}
                        temp_dic["provider"] = provider
                        data_dic.update(temp_dic)
                    if if_pay:
                        temp_dic = {}
                        temp_dic["if_pay"] = if_pay
                        data_dic.update(temp_dic)
                    self.parse_data(data_dic, project_name)

            except Exception as e:
                print(e)
                # self.driver.close()

    def parse_data(self, data_dic, project_name):
        res = rds_list.hgetall(project_name)
        if res:
            if not res.get("project_tags"):
                res["project_tags"] = ""
            if data_dic.get("style_tags"):
                if data_dic.get("style_tags") not in res["style_tags"]:
                    data_dic["style_tags"] = res["style_tags"] + "," + data_dic.get("style_tags")
            if data_dic.get("project_tags"):
                if data_dic.get("project_tags") not in res["project_tags"]:
                    data_dic["project_tags"] = res["project_tags"] + "," + data_dic.get("project_tags")
            if data_dic.get("provider"):
                if data_dic.get("provider") not in res["provider"]:
                    data_dic["provider"] = res["provider"] + "," + data_dic.get("provider")
            if data_dic.get("area"):
                if data_dic.get("area") not in res["area"]:
                    data_dic["area"] = res["area"] + "," + data_dic.get("area")
            for k in data_dic:
                if not data_dic[k]:
                    data_dic[k] = ""
            res.update(data_dic)
            rds_list.hmset(project_name, res)
        else:
            data = copy.deepcopy(self.one_video_dic)
            for k in data_dic:
                if not data_dic[k]:
                    data_dic[k] = ""
            data.update(data_dic)
            rds_list.hmset(project_name, data)

    def parse_single_data(self, data_dic, project_name):
        res = rds_single.hgetall(project_name)
        if res:
            if not res.get("project_tags"):
                res["project_tags"] = ""
            if data_dic.get("style_tags"):
                if data_dic.get("style_tags") not in res["style_tags"]:
                    data_dic["style_tags"] = res["style_tags"] + "," + data_dic.get("style_tags")
            if data_dic.get("project_tags"):
                if data_dic.get("project_tags") not in res["project_tags"]:
                    data_dic["project_tags"] = res["project_tags"] + "," + data_dic.get("project_tags")
            if data_dic.get("provider"):
                if data_dic.get("provider") not in res["provider"]:
                    data_dic["provider"] = res["provider"] + "," + data_dic.get("provider")
            for k in data_dic:
                if not data_dic[k]:
                    data_dic[k] = " "
            res.update(data_dic)
            try:
                # print(res)
                rds_single.hmset(project_name, res)
            except:
                pass
        else:
            data = copy.deepcopy(self.single_video_dic)
            for k in data_dic:
                if not data_dic[k]:
                    data_dic[k] = ""
            data.update(data_dic)
            rds_single.hmset(project_name, data)

    def video_list_api(self, data):
        headers = {
                "Host": "detail.mobile.youku.com",
                "Accept": "*/*",
                "Connection": "keep-alive",
                # "Cookie": "ouid=068a3e09fbd21065106848f11f652a92a0d56dc5; guid=068a3e09fbd21065106848f11f652a92a0d56dc5; P_deviceType=tablet; passport_sdk=iOS_1.8.0",
                "User-Agent": "Youku HD;8.2.0;iOS;13.3;iPad6,11",
                "Accept-Language": "zh-cn",
                "Referer": "https://www.youku.com",
                "Accept-Encoding": "gzip, deflate, br",
        }

        parser_dic = {
                "pg": "1",
                "ouid": "068a3e09fbd21065106848f11f652a92a0d56dc5",
                "idfa": "7AED33DD-0F97-418D-AFAA-72ED0578A41E",
                "vdid": "392FCF97-C853-46A3-B098-37FF3B01F5C2",
                "brand": "apple",
                "is_ret_all": "1",
                "os": "ios",
                "ver": "8.2.0",
                "deviceid": "0f607264fc6318a92b9e13c65db7cd1c",
                "operator": "",
                "guid": "7066707c5bdc38af1621eaf94a6fe734",
                "network": "WIFI",
                "pid": "87c959fb273378eb",
                "os_ver": "13.3",
                "btype": "iPad6,11",
                "utdid": "Xb7yx9orU%2B0DAMj%2FgHkahv0B",
                "_s_": "0e0a82e5e257f62bc3754af25a30d334 ",
                "_t_": "1577332494",
        }
        headers_web = {
                """"""
        }
        page_data = requests.get("https:" + data["url"])
        _s = re.findall("showid_en: \'(.*)\',", page_data.text)[0]
        extra_str = ""
        extra_str = "pg=1&ouid=068a3e09fbd21065106848f11f652a92a0d56dc5&idfa=7AED33DD-0F97-418D-AFAA-72ED0578A44E&vdid=392FCF97-C853-46A3-B098-37FF3B01F5C3&brand=apple&is_ret_all=1&os=ios&ver=8%2E2%2E0&deviceid=0f607264fc6318a92b9e13c65db7cd3c&operator=&guid=7066707c5bdc38af1621eaf94a6fe779&network=WIFI&pid=87c959fb273378eb&os_ver=13%2E3&btype=iPad6,11&utdid=Xb7yx9orU%2B0DAMj%2FgHkahv0B&_s_=62cc57d8d93359d970060d389d170c66&_t_=1577333288"
        get_url = "https://detail.mobile.youku.com/shows/{0}/reverse/videos?{1}".format(_s, extra_str)

        requests_res = requests.get(get_url, headers=headers)
        print(requests_res.text)
        res_json = requests_res.json()
        data_list = res_json["results"]
        for one_data in data_list:
            video_title = one_data["title"]
            describe = one_data["desc"]
            duration = one_data["duration"]
            play_count = one_data["total_pv"]
            comment_count = one_data["total_comment"]
            tags = one_data["tags"]
            vid = one_data["videoid"]
            video_count = one_data["show_videostage"]
            url = "https://v.youku.com/v_show/id_%s" % vid
            dic = {
                    "video_title": video_title,
                    "describe": describe,
                    "duration": duration,
                    "play_count": play_count,
                    "play_heat": data["play_count"],
                    "comment_count": comment_count,
                    "tags": tags,
                    "vid": vid,
                    "video_url": url,
                    "video_count": video_count,
                    "url": data["url"],
                    "album": data["title"]
            }
            self.parse_single_data(dic, url[6:])

    def func_video_page_api(self, task=0):
        has_data = rds_get.dbsize()
        while has_data:
            keys = rds_get.randomkey()
            res = rds_get.hgetall(keys)
            # res = rds_get.hgetall("iqiyi_100010201")
            has_data = rds_get.dbsize()
            if res["access"] == "deny":
                rds_get.delete(keys)
                continue
            # time.sleep(0.2)
            try:
                print(res["url"])
                self.video_list_api(res)
                # self.detail_page_api(res)
                rds_get.delete(keys)
            except Exception as e:
                print(e, res["url"])

    def get_page_list(self, data):
        headers = {

                "accept": "*/*",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "zh,zh-CN;q=0.9",
                "cookie": "__ysuid=1553755022290AhL; juid=01d9bjgm0l2ngc; cna=2U8aFb1yaVcCAdr3nSX3f47K; __aryft=1557994090; __artft=1557994090; UM_distinctid=16eea2ee55739c-08e02b1a26b73c-2393f61-161012-16eea2ee55881b; ykss=f0cdf05d77e5a6dcebeb4c1c; __ayft=1576549468599; __aysid=1577241653062aoh; __ayscnt=5; yseid=1577241727003hUSqrH; yseidcount=4; ycid=0; __arycid=dz-3-00; __arcms=dz-3-00; referhost=https%3A%2F%2Flist.youku.com; _m_h5_c=60b9e2b4228097503d3975caca016d24_1577269476232%3B6030e92d9f896f1b7024ac8e5df7c81a; P_ck_ctl=70E1D32F5B5E92006640274BCF8D7371; _m_h5_tk=92bfeed90e6fedabcac24cf2fbc211de_1577268775512; _m_h5_tk_enc=9c860e1c7cdd927ab133515c7922f98e; CNZZDATA1277955961=1269611647-1575885450-https%253A%252F%252Flist.youku.com%252F%7C1577259749; seid=01dsu50o381d4f; __arpvid=15772649493186fuodZ-1577264949336; __aypstp=141; __ayspstp=80; seidtimeout=1577266751889; ypvid=1577264954182j6rlMt; ysestep=32; yseidtimeout=1577272154186; ystep=41; __ayvstp=95; __aysvstp=95; isg=BE1Nn-BVLy9TNInF8eGG1rJNXGkHgr-WrpQ5v4_I4OQxhm44V3_MzRVQ9FJFRpm0",
                "referer": "https://v.youku.com/",
                "sec-fetch-mode": "no-cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36",

        }
        page = 1
        while True:
            page_info_text = requests.get("http:" + data["url"], headers=headers).text
            # print(page_info_text)
            vid = re.findall("videoId: \'(\d*)\'", page_info_text)[0]
            showid = re.findall("showid: \'(\d*)\'", page_info_text)[0]
            encode_id = data["id"]
            pm = re.findall("playmode: \'(\d*)\'", page_info_text)[0]
            cat_id = re.findall("catId: \'(\d*)\'", page_info_text)[0]
            componentid = re.findall('"componentId":(\d*)', page_info_text)[0]
            isSimple = re.findall("isSimple: \'(.*)\'", page_info_text)[0]
            parser_dic = {
                    "l": "debug",
                    "pm": pm,
                    "vid": vid,
                    "fid": "0",
                    "showid": showid,
                    "sid": "0",
                    "componentid": componentid,
                    "videoCategoryId": cat_id,
                    "isSimple": isSimple,
                    "videoEncodeId": encode_id,
                    "page": page,
            }
            page_html = requests.get("https://v.youku.com/page/playlist?%s" % urllib.parse.urlencode(parser_dic),
                                     headers=headers)
            page += 1
            page_json = page_html.json()
            # print(page_html.json)
            if page_json["html"] == "\n":
                break
            soup = BeautifulSoup(page_json["html"], 'lxml')
            # print(soup)
            # soup.contents
            dev_list = soup.find_all(attrs={"class": "item item-cover"})
            for dev in dev_list:
                video_title = dev.get("title")
                vid = dev.get("item-id")
                video_url = dev.a.get("href")
                dev_text = dev.text
                if "VIP" in dev_text:
                    if_pay = "VIP"
                else:
                    if_pay = ""
                play_count = re.findall("热度 (\d+)", dev_text)[0]
                try:
                    duration = re.findall('(\d+:\d+:\d+)', dev_text)[0]
                except:
                    duration = re.findall('(\d+:\d+)', dev_text)[0]
                # print(dev.get_text)
                dic = {
                        "video_title": video_title,
                        "duration": trans_duration(duration),
                        "play_count": play_count,
                        "if_pay": if_pay,
                        "video_url": "https:" + video_url,
                        "url": "https:" + data["url"],
                        "vid": vid,
                        "album": data["title"]

                }
                self.parse_single_data(dic, video_url)

    def requests_video_page(self, task=0):
        # self.driver = webdriver.Chrome(options=self.chrome_options)
        # self.driver.maximize_window()
        has_data = rds_get.dbsize()
        headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "zh-CN,zh;q=0.9",
                "cache-control": "max-age=0",
                "cookie": "_m_h5_tk=73a70f7de7d2320b02ba9ec5e0efe40d_1577284067308; _m_h5_tk_enc=8249085dd3a0cb150e3770775414d9d9; UM_distinctid=16f3d27fc52462-093ef1e38c846f-2393f61-161012-16f3d27fc53a00; CNZZDATA1277955961=292254450-1577278934-%7C1577278934; juid=01dsuifv342148; seid=01dsuifv354uv; referhost=; seidtimeout=1577280828325; cna=M0yKFuVwuiUCAdr3nSXb7S67; ypvid=1577279030379SiTUmn; yseid=1577279030380MlQSc6; ysestep=1; yseidcount=1; yseidtimeout=1577286230381; ycid=0; ystep=1; __ysuid=1577279030556EXq; __ayft=1577279030559; __aysid=1577279030559SBp; __ayscnt=1; __ayvstp=3; __aysvstp=3; __arpvid=1577279038233WZL7FT-1577279038357; __arycid=dw-3-00; __arcms=dw-3-00; __aypstp=2; __ayspstp=2; _m_h5_c=e100b1be71c7d1613eab9a10434a6edb_1577286238870%3B11698b316b4f653e59d9fa764c137c9a; isg=BGdnS-8H9X3btHHxdz4mIP8y9psx7DvOMApjLTnUg_YdKIfqQbzLHqU6TGkTwBNG",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36",

        }
        while has_data:
            try:
                keys = rds_get.randomkey()
                res = rds_get.hgetall(keys)
                has_data = rds_get.dbsize()
                page_data = requests.get(res["url"], headers=headers)
                print("task ", task, keys)
                # print(page_data.text)
                page_json_re = re.findall("__INITIAL_DATA__ =(.*);</script>", page_data.text)
                page_json = json.loads(page_json_re[0], encoding="utf-8")
                father_id = res["url"].split("_", -1)
                father_id = "youku_" + father_id[-1]
                father_id = father_id.split(".")[0]
                uploadDate = re.findall('\"uploadDate\" content=\"(.*)\"', page_data.text)[0]
                datePublished = re.findall('\"datePublished\" content=\"(.*)\"', page_data.text)[0]
                rate = page_json["data"]["data"]["nodes"][0]["nodes"][0]["nodes"][0]["data"].get("socreValue")
                description = page_json["data"]["data"]["nodes"][0]["nodes"][0]["nodes"][0]["data"].get("desc")
                year = page_json["data"]["data"]["nodes"][0]["nodes"][0]["nodes"][0]["data"].get("showReleaseYear")
                source_url = ""
                tmep_dic = {
                        "rate": rate,
                        "description": description,
                        "year": year
                }
                single_data_dic = {
                        "uploadDate": uploadDate,
                        "datePublished": datePublished
                }
                self.parse_data(tmep_dic, father_id)
                self.parse_single_data(single_data_dic, keys)
                rds_get.delete(keys)
            except:
                print("error", keys)
                continue

    def video_page(self, task=0):
        self.driver = webdriver.Chrome(options=self.chrome_options)
        self.driver.maximize_window()
        has_data = rds_get.dbsize()
        while has_data:
            keys = rds_get.randomkey()
            res = rds_get.hgetall(keys)
            has_data = rds_get.dbsize()
            # print(has_data)
            # print(res)
            # self.driver.execute_script('window.open("%s");' % res["url"])
            # 关闭到当前窗口
            # self.driver.close()
            time.sleep(0.2)
            # for handle in self.driver.window_handles:
            #     self.driver.switch_to.window(handle)
            try:
                self.driver.get(res["video_url"])
                self.driver.execute_script("window.scrollBy(0,1000)")
                self.driver.implicitly_wait(10)
                page_text = self.driver.page_source
                uploadDate = re.findall('\"uploadDate\" content=\"(.*)\"', page_text)[0]
                datePublished = re.findall('\"datePublished\" content=\"(.*)\"', page_text)[0]
                source_url_obj = self.driver.find_element_by_xpath("//div[@class='title-wrap']")
                source_url = source_url_obj.find_element_by_xpath(".//a").get_attribute("href")
                father_id = res["url"].split("_", -1)
                father_id = "youku_" + father_id[-1]
                father_id = father_id.split(".")[0]
                tmep_dic = {
                        "source_url": source_url
                }
                self.parse_data(tmep_dic, father_id)
                time.sleep(2)
                comment_count = self.driver.find_element_by_xpath("//em[@id='allCommentNum']").text
                # print(describe)

                dic = {"uploadDate": uploadDate,
                       "datePublished": datePublished,
                       "comment_count": comment_count.replace("(", "").replace(")", ""),
                       }
                self.parse_single_data(dic, keys)
                # rds_get.delete(keys)
            except:
                continue

    def requests_video_comment(self, task=0):
        # self.driver = webdriver.Chrome(options=self.chrome_options)
        # self.driver.maximize_window()
        has_data = rds_get.dbsize()
        headers = {

                "accept": "*/*",
  "accept-encoding": "gzip, deflate, br",
  "accept-language": "zh,zh-CN;q=0.9",
  "cookie": '__ysuid=1553755022290AhL; juid=01d9bjgm0l2ngc; cna=2U8aFb1yaVcCAdr3nSX3f47K; __aryft=1557994090; __artft=1557994090; UM_distinctid=16eea2ee55739c-08e02b1a26b73c-2393f61-161012-16eea2ee55881b; ykss=f0cdf05d77e5a6dcebeb4c1c; __ayft=1576549468599; __aysid=1577241653062aoh; __ayscnt=5; yseid=1577241727003hUSqrH; yseidcount=4; ycid=0; _m_h5_c=60b9e2b4228097503d3975caca016d24_1577269476232%3B6030e92d9f896f1b7024ac8e5df7c81a; __arycid=dz-3-00; __arcms=dz-3-00; P_ck_ctl=3724C216478E93AE9C70A18917CA4435; referhost=https%3A%2F%2Fv.youku.com; _m_h5_tk=ed8d26a60dfb065fb8363be3593aabe9_1577343047627; _m_h5_tk_enc=56d03918544e1a94aadb92bebb2f7ae7; seid=01dt0d9o0h440; __arpvid=1577342442086qEZat9-1577342442109; __aypstp=277; __ayspstp=216; seidtimeout=1577344245142; ypvid=1577342447564rTks23; ysestep=133; yseidtimeout=1577349647566; ystep=142; __ayvstp=1319; __aysvstp=1319; isg=BDQ0ZkdZ1mMf1UBucOYP7YNSBfJmpWadP_swVM6VbL9vOdSD9hzhhdD_vTlEwZBP',
  "referer": "https://v.youku.com/v_show/id_XMzY0NjcwMDI1Ng==.html?spm=a2h0k.11417342.soresults.dtitle&s=ae2f076a7e274ecdb2df",
  "sec-fetch-mode": "no-cors",
  "sec-fetch-site": "same-site",
  "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36"

        }
        while has_data:
            try:
                keys = rds_get.randomkey()
                res = rds_get.hgetall(keys)
                has_data = rds_get.dbsize()
                page_data = requests.get(res["video_url"])
                _s = re.findall("videoId: \'(.*)\',", page_data.text)[0]
                url = "https://p.comments.youku.com/ycp/comment/pc/commentList?app=100-DDwODVkv&objectId={0}&objectType=1&listType=0&currentPage=1&pageSize=30&sign=71bbe19a3eccfd7867229821e4e069df&time=1577342452".format(
                    _s)
                page_data = requests.get(url, headers=headers)
                print("task ", task, keys)
                # print(page_data.text)
                page_json = page_data.json()
                comment_count = page_json["data"]["totalSize"]
                single_data_dic = {
                        "comment_count": comment_count,
                }
                self.parse_single_data(single_data_dic, keys)
                rds_get.delete(keys)
            except:
                print("error", keys)
                continue


if __name__ == "__main__":
    crawler_youku = CrawlerMain()
    area_list = [
            "中国", "美国", "英国", "日本", "中国香港", "法国", "中国台湾", "加拿大", "澳大利亚", "德国", "韩国", "俄罗斯", "瑞士", "西班牙",
            "丹麦", "墨西哥", "新加坡", "印度", "以色列", "其他"
    ]
    style_tags_list = [
            "人物", "军事", "历史", "自然", "古迹", "探险", "科技", "文化", "刑侦", "社会", "旅游"
    ]
    provider_lsit = [
            "优酷", "Discovery", "BBC", "美国国家地理", "美国历史频道", "上海纪实频道", "五星传奇", "IMG"
    ]
    if_pay_list = [
            "1", "2"
    ]
    # for area in area_list:
    #     crawler_youku.list_page(area=area)
    # for style_tags in style_tags_list:
    #     crawler_youku.list_page(style_tags=style_tags)
    # for provider in provider_lsit:
    #     crawler_youku.list_page(provider=provider)
    # for if_pay in if_pay_list:
    #     crawler_youku.list_page(if_pay=if_pay)
    # revise_data()

    executor = ProcessPoolExecutor(max_workers=8)
    futures = []
    crlawler_bilbili = CrawlerMain()
    # process_task = crlawler_bilbili.video_page
    for one_scan in range(8):
        crawler_youku = CrawlerMain()
        future = executor.submit(crawler_youku.requests_video_comment, task=one_scan)
        futures.append(future)

        # crlawler_bilbili.video_page(one_scan)
    executor.shutdown(True)

    # crawler_youku.func_video_page_api()
    # crawler_youku.requests_video_page()
    # crawler_youku.video_page()
    # crawler_youku.requests_video_comment()
