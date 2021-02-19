# -*- coding:utf-8 -*-
# @Time : 2019/12/11 18:58
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

# rds_list = redis.StrictRedis(host='127.0.0.1', port=6379, db=1, decode_responses=True)
# rds_single = redis.StrictRedis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
# rds_get = redis.StrictRedis(host='127.0.0.1', port=6379, db=15, decode_responses=True)

# rds_copy = redis.StrictRedis(host='127.0.0.1', port=6379, db=1, decode_responses=True)
rds_list = redis.StrictRedis(host='192.168.17.60', port=6379, db=1, decode_responses=True)
rds_single = redis.StrictRedis(host='192.168.17.60', port=6379, db=0, decode_responses=True)
rds_get = redis.StrictRedis(host='192.168.17.60', port=6379, db=15, decode_responses=True)


def revise_data():
    scan_re = rds_list.scan_iter()
    for one_scan in scan_re:
        # print(one_scan)
        data = rds_list.hgetall(one_scan)
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
        self.driver = webdriver.Chrome(options=self.chrome_options)
        self.one_video_dic = {
                "platform": "tencnt_video",
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
                "platform": "tencnt_video",
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
                  if_free=None,
                  title=None,
                  describe=None,
                  project_tag_xpath=None,
                  provider=None,
                  year=None,
                  next_page_xpath=None,
                  roll=None,
                  project_tag=None,
                  style_tags=None
                  ):

        while True:
            self.driver.get(releaserUrl)
            # self.driver.implicitly_wait(5)
            time.sleep(0.2)
            js = "var q=document.documentElement.scrollTop=%s" % roll
            max_video_num = self.driver.find_element_by_xpath("//span[@class='hl']").text
            print(int(max_video_num))
            max_page = int(int(max_video_num) / 28)
            for p in range(max_page):
                roll += roll
                js = "var q=document.documentElement.scrollTop=%s" % roll
                self.driver.execute_script(js)
                time.sleep(0.3)
            vidoe_list_obj = self.driver.find_elements_by_xpath(
                    "//body[@class='page_channel page_channel_doco']/div[@class='mod_row_box']/div[@class='mod_bd _mod_listpage']/div[@class='mod_figure mod_figure_v_default mod_figure_list_box']/div")

            try:
                for count, one_video in enumerate(vidoe_list_obj):
                    print(count)
                    if_pay = ""
                    video_count = ""
                    describe = ""
                    title = one_video.find_element_by_xpath("./div[1]//a[1]").text
                    project_name = "tencent_%s" % title
                    url = one_video.find_element_by_xpath("./a[1]").get_attribute('href')
                    video_count_list = one_video.find_elements_by_xpath("./a[1]/div[1]")
                    discribe_list = one_video.find_elements_by_xpath("./div[1]/div[1]")
                    if discribe_list:
                        describe = discribe_list[0].text
                    if video_count_list:
                        video_count = video_count_list[0].text
                    if_pay_list = one_video.find_elements_by_xpath("./a[1]/img[2]")
                    if if_pay_list:
                        if_pay = if_pay_list[0].get_attribute("alt")
                    data_dic = {
                            "url": url,
                            "title": title,
                            "video_count": video_count,
                            "if_pay": if_pay,
                            "describe": describe
                    }
                    # action = ActionChains(self.driver)
                    # action.click(one_video).perform()
                    if style_tags:
                        temp_dic = {}
                        temp_dic["style_tags"] = style_tags
                        data_dic.update(temp_dic)
                    if project_tag:
                        temp_dic = {}
                        temp_dic["project_tags"] = project_tag
                        data_dic.update(temp_dic)
                    if year:
                        temp_dic = {}
                        temp_dic["year"] = year
                        data_dic.update(temp_dic)
                    if provider:
                        temp_dic = {}
                        temp_dic["provider"] = provider
                        data_dic.update(temp_dic)
                    self.parse_data(data_dic, project_name)
                else:
                    # self.driver.close()
                    break
            except Exception as e:
                print(e)
                self.driver.close()

    def style_tags(self, releaserUrl=None,
                   video_list_xpath=None,
                   play_count_xpath=None,
                   if_free=None,
                   title=None,
                   describe=None,
                   style_tags_xpath=None,
                   style=None,
                   project_tag=None,
                   provider=None,
                   year=None,
                   next_page_xpath=None,
                   roll=None, ):
        self.driver.get(releaserUrl)
        time.sleep(0.2)
        style_tags_obj = self.driver.find_elements_by_xpath(style_tags_xpath)
        for style_tag in style_tags_obj:
            style_tags = style_tag.text
            print(style_tags)
            if style_tags == "全部":
                continue
            action = ActionChains(self.driver)
            action.click(style_tag).perform()
            provider = style_tags
            # if True:
            self.list_page(releaserUrl=self.driver.current_url,
                           video_list_xpath=video_list_xpath,
                           play_count_xpath=play_count_xpath,
                           if_free=if_free,
                           title=title,
                           describe=describe,
                           project_tag_xpath=style_tags_xpath,
                           project_tag=project_tag,
                           style_tags=style,
                           provider=provider,
                           year=year,
                           next_page_xpath=next_page_xpath,
                           roll=roll, )

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
            if data_dic.get("if_pay"):
                if data_dic.get("if_pay") not in res["if_pay"]:
                    data_dic["if_pay"] = res["if_pay"] + "," + data_dic.get("if_pay")
            for k in data_dic:
                if not data_dic[k]:
                    data_dic[k] = ""
            res.update(data_dic)
            rds_list.hmset(project_name, res)
        else:
            data = copy.deepcopy(self.one_video_dic)
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
                    data_dic[k] = ""
            res.update(data_dic)
            rds_single.hmset(project_name, res)
        else:
            data = copy.deepcopy(self.single_video_dic)
            for k in data_dic:
                if not data_dic[k]:
                    data_dic[k] = " "
            data.update(data_dic)
            rds_single.hmset(project_name, data)

    def video_page(self, task=0):
        # self.driver = webdriver.Chrome(options=self.chrome_options)
        # self.driver.maximize_window()
        has_data = rds_get.dbsize()
        while has_data:
            keys = rds_get.randomkey()
            res = rds_get.hgetall(keys)
            has_data = rds_get.dbsize()
            time.sleep(0.2)
            url_id_list = re.findall("cover/(.*).html", res["url"])
            if url_id_list:
                url_id = url_id_list[0]
            else:
                print(res)
                url_id = ""
            # for handle in self.driver.window_handles:
            #     self.driver.switch_to.window(handle)
            headers = {
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                    "accept-encoding": "gzip, deflate, br",
                    "accept-language": "zh,zh-CN;q=0.9",
                    # "cookie": "pgv_pvi=203414528; RK=SCQYJhGMVf; ptcz=5f0818b08a7345580a07bce669e0f0468b64107f4ecfb2c9bebf109cb23cf4fb; pgv_pvid=2754880744; ts_uid=176985184; tvfe_boss_uuid=54e907210062ff55; video_guid=0df27917cdb73abd; video_platform=2; XWINDEXGREY=0; mobileUV=1_16ac3c085a7_484c1; tvfe_search_uid=acc18029-4786-42c4-8f6a-f308777454bc; Qs_lvt_311470=1562066061; Qs_pv_311470=992309958717814400; _ga=GA1.2.1184421010.1562066062; login_remember=qq; ptui_loginuin=593516104; o_cookie=593516104; bucket_id=9231005; pac_uid=1_593516104; pgv_si=s4148599808; ptisp=; pgv_info=ssid=s6122922102; ts_refer=m.v.qq.com/x/page/o/d/k/i3023rbj8yk.html; qv_als=Kg5QkSIirE60EDyzA11576570304alZX8g==; ptag=m_v_qq_com|videolist:title; ts_last=v.qq.com/detail/m/mzc00200iwawqac.html; ad_play_index=132",
                    # "referer": "https://v.qq.com/x/cover/%s.html" % url_id,
                    "sec-fetch-mode": "navigate",
                    "sec-fetch-site": "same-origin",
                    "upgrade-insecure-requests": "1",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36",
                    "sec-fetch-user": "?1",
                    "cache-control": "max-age=0",
                    "if-modified-since": "Thu, 19 Dec 2019 02:30:00 GMT"
            }
            try:
                page_data = requests.get(res["url"], headers=headers,timeout=5)
            except:
                continue
            # print(page_data.status_code)
            if page_data.status_code != 200:
                rds_get.delete(keys)
                continue
            res_text = page_data.content.decode("utf-8")
            # print(page_data.content.decode("utf-8"))
            print("task %s" % task,res["url"])

            try:
                cover_info = re.findall("var COVER_INFO = (.*)\n", res_text)[0]
                cover_info_dic = json.loads(cover_info,encoding="utf-8")
            except:
                cover_info_dic = {}
            try:
                VIDEO_INFO = re.findall("var VIDEO_INFO = (.*)\n", res_text)[0]
                video_info_dic = json.loads(VIDEO_INFO,encoding="utf-8")
            except Exception as e:
                print("video_dic_erro", e)
                rds_get.delete(keys)
                continue
            # if r"\x" in str(cover_info_dic):
            #     # print(video_info_dic.encoding("utf-8"))
            #     continue
            if cover_info_dic.get("video_ids"):
                video_id_list = cover_info_dic.get("video_ids")
                # if_pay_list = cover_info_dic.get("vip_ids")
                langue = cover_info_dic.get("langue")
                if not langue:
                    langue = ""
                id = url_id
                year = cover_info_dic.get("year")
                if not year:
                    year = cover_info_dic.get("yearAndArea").get("year")
                if not year:
                    year = cover_info_dic.get("publish_date")
                area = cover_info_dic.get("area_name")
                if not area:
                    area = ""
                style_tags = cover_info_dic.get("main_genre")
                if not style_tags:
                    style_tags = ""
                description = cover_info_dic.get("description")
                if not description:
                    description = ""
                play_count_sum = cover_info_dic.get("view_all_count")
                if not play_count_sum:
                    play_count_sum = ""
                try:
                    rate = cover_info_dic.get("score").get("score")
                except:
                    rate = ""
                if not rate:
                    rate = ""
                title = cover_info_dic.get("title")
                dic = {"description": description,
                       "rate": rate,
                       "play_count_sum": play_count_sum,
                       "langue": langue,
                       "id": id,
                       "year": year,
                       "area": area,
                       "style_tags": style_tags,
                       "title": title,
                       "video_count": len(video_id_list)
                       }
                # print(dic)
                project_name = keys
                # print(dic)

                self.parse_data(dic, project_name)
                one_video_dic = {}
                for count, video in enumerate(video_id_list):
                    pay_dic = {}
                    pay_dic["vid"] = video
                    pay_dic["video_count"] = count + 1
                    pay_dic["album"] = title
                    pay_dic["url"] = res["url"]
                    one_video_dic[video] = pay_dic
                self.one_video_page(title, one_video_dic, type="list")
            else:
                video_id_list = []
                video_id_list.append(video_info_dic.get("vid"))
                duration = video_info_dic.get("duration")
                if not duration:
                    duration = ""
                year = video_info_dic.get("publish_date")
                if not year:
                    year = ""
                play_count_sum = video_info_dic.get("view_all_count")
                if not play_count_sum:
                    play_count_sum = ""
                project_tags = video_info_dic.get("pioneer_tag")
                if not project_tags:
                    project_tags = ""
                title = video_info_dic.get("title")
                try:
                    comment_count = re.findall("title='(\d+)热评'", res)[0]
                except:
                    comment_count = 0
                dic = {
                        "play_count_sum": play_count_sum,
                        "duration": duration,
                        "project_tags": project_tags,
                        "title": title,
                        "year": year,
                        "video_count": 1,
                        "comment_count": comment_count
                }
                project_name = keys
                self.parse_data(dic, project_name)
                dic["album"] = dic["title"]
                dic["video_url"] = res["url"]
                dic["video_id"] = res["title"]
                dic["play_count"] = res["play_count_sum"]
                one_video_dic = {
                        title: dic
                }
                # print(one_video_dic)
                self.one_video_page(title, one_video_dic, type="single")
            rds_get.delete(keys)

    def one_video_page(self, title, one_video_dic, type="list"):
        for one_video in one_video_dic:
            if type == "list":
                url = one_video_dic[one_video]["url"]
                headers = {
                        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                        "accept-encoding": "gzip, deflate, br",
                        "accept-language": "zh,zh-CN;q=0.9",
                        # "cookie": "pgv_pvi=203414528; RK=SCQYJhGMVf; ptcz=5f0818b08a7345580a07bce669e0f0468b64107f4ecfb2c9bebf109cb23cf4fb; pgv_pvid=2754880744; ts_uid=176985184; tvfe_boss_uuid=54e907210062ff55; video_guid=0df27917cdb73abd; video_platform=2; XWINDEXGREY=0; mobileUV=1_16ac3c085a7_484c1; tvfe_search_uid=acc18029-4786-42c4-8f6a-f308777454bc; Qs_lvt_311470=1562066061; Qs_pv_311470=992309958717814400; _ga=GA1.2.1184421010.1562066062; login_remember=qq; ptui_loginuin=593516104; o_cookie=593516104; bucket_id=9231005; pac_uid=1_593516104; pgv_si=s4148599808; ptisp=; pgv_info=ssid=s6122922102; ts_refer=m.v.qq.com/x/page/o/d/k/i3023rbj8yk.html; qv_als=Kg5QkSIirE60EDyzA11576570304alZX8g==; ptag=m_v_qq_com|videolist:title; ts_last=v.qq.com/detail/m/mzc00200iwawqac.html; ad_play_index=132",
                        "referer": url,
                        "sec-fetch-mode": "navigate",
                        "sec-fetch-site": "same-origin",
                        "upgrade-insecure-requests": "1",
                        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36",
                }
                # print(url)
                url_lis = url.split(".", -1)
                url_lis[2] = url_lis[2] + "/" + one_video
                single_data_url = ".".join(url_lis)
                # print(single_data_url)
                page_data = requests.get(single_data_url, headers=headers,timeout=5)
                if page_data.status_code != 200:
                    return None
                VIDEO_INFO = re.findall("var VIDEO_INFO = (.*)\n", page_data.content.decode("utf-8"))[0]
                video_info_dic = json.loads(VIDEO_INFO,encoding="utf-8")
                video_id = video_info_dic.get("vid")
                duration = video_info_dic.get("duration")
                year = video_info_dic.get("video_checkup_time")
                play_count_sum = video_info_dic.get("view_all_count")

                project_tags = video_info_dic.get("pioneer_tag")
                if not project_tags:
                    project_tags = ""
                video_title = video_info_dic.get("title")
                try:
                    comment_count = re.findall("title='(\d+)热评'", page_data.text)[0]
                except:
                    comment_count = 0
                project_name = "tencnt_video_%s" % (one_video)
                if_pay = video_info_dic.get("pay_status")
                if not if_pay:
                    if_pay = ""
                dic = {
                        "album": title,
                        "video_title": video_title,
                        "if_pay": if_pay,
                        "comment_count": comment_count,
                        "url": url,
                        "video_url": single_data_url,
                        "video_id": video_id,
                        "duration": duration,
                        "video_count": 1,
                        "play_count": play_count_sum,
                        "year": year,
                        "project_tags": project_tags,
                }

                # print(dic)
                self.parse_single_data(dic, project_name)
            else:
                project_name = "tencnt_video_%s" % (one_video)
                dic = one_video_dic[one_video]
                self.parse_single_data(dic, project_name)

    def detail_page(self):
        pass


if __name__ == "__main__":
    crlawler_tenxun = CrawlerMain()
    provider_dic = {
            "全部": "https://v.qq.com/channel/doco?_all=1&channel=doco&itrailer=-1&listpage=1&pay=-1&sort=19",
            "BBC":"https://v.qq.com/channel/doco?_all=1&channel=doco&itrailer=1&listpage=1&pay=-1&sort=19",
            "国家地理":"https://v.qq.com/channel/doco?_all=1&channel=doco&itrailer=4&listpage=1&pay=-1&sort=19",
            "HBO":"https://v.qq.com/channel/doco?_all=1&channel=doco&itrailer=3175&listpage=1&pay=-1&sort=19",
            "NHK":"https://v.qq.com/channel/doco?_all=1&channel=doco&itrailer=2&listpage=1&pay=-1&sort=19",
            "历史频道":"https://v.qq.com/channel/doco?_all=1&channel=doco&itrailer=7&listpage=1&pay=-1&sort=19",
            "ITV":"https://v.qq.com/channel/doco?_all=1&channel=doco&itrailer=3530&listpage=1&pay=-1&sort=19",
            "探索频道":"https://v.qq.com/channel/doco?_all=1&channel=doco&itrailer=3174&listpage=1&pay=-1&sort=19",
            "ZDF":"https://v.qq.com/channel/doco?_all=1&channel=doco&itrailer=3176&listpage=1&pay=-1&sort=19",
            "ARTE":"https://v.qq.com/channel/doco?_all=1&channel=doco&itrailer=3172&listpage=1&pay=-1&sort=19",
            "腾讯自制":"https://v.qq.com/channel/doco?_all=1&channel=doco&itrailer=15&listpage=1&pay=-1&sort=19",
            "合作机构":"https://v.qq.com/channel/doco?_all=1&channel=doco&itrailer=6&listpage=1&pay=-1&sort=19",
            "其他":"https://v.qq.com/channel/doco?_all=1&channel=doco&itrailer=5&listpage=1&pay=-1&sort=19",
    }
    for provider in provider_dic:
        crlawler_tenxun.list_page(releaserUrl=provider_dic[provider],
                               video_list_xpath="/html[1]/body[1]/div[6]/div[1]/div[2]/div",
                               roll=1000,
                               provider=provider
                               )

    # executor = ProcessPoolExecutor(max_workers=8)
    # futures = []
    # crlawler_tecnt = CrawlerMain()
    # # process_task = crlawler_bilbili.video_page
    # for one_scan in range(8):
    #     crlawler_tecnt = CrawlerMain()
    #     future = executor.submit(crlawler_tecnt.video_page,task=one_scan)
    #     futures.append(future)
    #
    #     # crlawler_bilbili.video_page(one_scan)
    # executor.shutdown(True)
    # crlawler_tecnt = CrawlerMain()
    # crlawler_tecnt.video_page()
    # revise_data()
