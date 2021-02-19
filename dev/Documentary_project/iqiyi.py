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
        # prefs = {"profile.managed_default_content_settings.images": 2}
        # self.chrome_options.add_experimental_option("prefs", prefs)
        # self.driver = webdriver.Chrome(options=self.chrome_options)
        self.one_video_dic = {
                "platform": "iqiyi_video",
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
                "platform": "iqiyi_video",
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
                  style_tags=None,
                  category_id=""
                  ):
        page = 1
        total_page = 10
        while True:
            headers = {
                    "accept": "*/*",
                    "accept-encoding": "gzip, deflate, br",
                    "accept-language": "zh,zh-CN;q=0.9",
                    "content-type": "application/x-www-form-urlencoded",
                    "cookie": "QP001=1; QP0017=100; QP0018=100; QC005=c48ede3bf6d027f313486d3475d317b7; QC006=d6c2a542ed01ba7953ae094d36a40083; T00404=c8c154e05c0e54572a5eb20047dd6129; QC173=0; QP0013=; Hm_lvt_53b7374a63c37483e5dd97d78d9bb36e=1574822745,1575886435,1576549484; QC007=DIRECT; QC008=1556242615.1576549484.1576549484.9; nu=0; QC175=%7B%22upd%22%3Atrue%2C%22ct%22%3A%22%22%7D; QILINPUSH=1; T00700=EgcI77-tIRAF; QC159=%7B%22color%22%3A%22FFFFFF%22%2C%22channelConfig%22%3A1%2C%22isOpen%22%3A1%2C%22speed%22%3A13%2C%22density%22%3A30%2C%22opacity%22%3A86%2C%22isFilterColorFont%22%3A1%2C%22proofShield%22%3A0%2C%22forcedFontSize%22%3A24%2C%22isFilterImage%22%3A1%2C%22hadTip%22%3A1%7D; QP007=720; Hm_lpvt_53b7374a63c37483e5dd97d78d9bb36e=1576736529; IMS=IggQCRj_vO7vBSokCiBiYWNlNmIxMWExZjJmOTE4NzFmMjc3M2FmMTE0ZDUyYRAA; QC010=14783026; __dfp=a0c95636e67e384d24a08f486f2e6af20530557a862431f4441f2f23083da385e8@1577845483549@1576549484549",
                    "origin": "https://list.iqiyi.com",
                    # "referer": "https://list.iqiyi.com/www/3/-------------24-1-1-iqiyi-1-.html",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-site",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36",

            }
            quary = {
                    "access_play_control_platform": "14",
                    "channel_id": "3",
                    "data_type": "1",
                    "from": "pcw_list",
                    "is_album_finished": "",
                    "is_purchase": "",
                    # "is_purchase": "0",
                    # "is_purchase": "2",
                    "key": "",
                    "market_release_date_level": "",
                    # "mode": "24",
                    # "mode": "4",
                    "mode": "11",
                    "pageNum": str(page),
                    "pageSize": "1500",
                    "site": "iqiyi",
                    "source_type": "",
                    # "three_category_id": "70;must",
                    # "without_qipu": "1",
                    "three_category_id": "%s;must" % category_id,
                    "without_qipu": "1",
            }
            if page >= 1 and page <= total_page:
                requests_url = "https://pcw-api.iqiyi.com/search/video/videolists?%s" % urllib.parse.urlencode(quary)
                res = requests.get(requests_url, headers=headers)
                print("get data at page %s" % page)
                page += 1
            else:
                break
            res_json = res.json()
            # print(res_json)
            vidoe_list_obj = res_json["data"]["list"]
            total_page = res_json["data"]["pageTotal"] + 1
            try:
                for count, one_video in enumerate(vidoe_list_obj):
                    print(count)
                    area = ""
                    style_tag = ""
                    if_pay = one_video.get("payMark")
                    id = one_video["albumId"]
                    title = one_video["name"]
                    project_name = "iqiyi_%s" % id
                    url = one_video["playUrl"]
                    video_count = one_video["videoCount"]
                    try:
                        describe = one_video["description"]
                    except:
                        describe = one_video["focus"]

                    score = one_video.get("score")
                    style_tag_list = one_video["categories"]
                    if style_tag_list:
                        for l in style_tag_list:
                            try:
                                subName = l["subName"]
                                if subName == "地区":
                                    area = l["name"]
                                elif subName == "类型":
                                    style_tag += l["name"] + ","
                                elif subName == "出品方":
                                    provider = l["name"] + ","
                            except:
                                style_tag = l["name"] + ","
                    formatPeriod = one_video.get("formatPeriod")
                    issueTime = one_video.get("issueTime")
                    if issueTime:
                        issueTime = datetime.datetime.fromtimestamp(issueTime / 1000).strftime("%Y-%m-%d %H:%M:%S")
                    data_dic = {
                            "id": id,
                            "url": url,
                            "title": title,
                            "video_count": video_count,
                            "if_pay": if_pay,
                            "describe": describe,
                            "issueTime": issueTime,
                            "formatPeriod": formatPeriod,
                            "score": score,
                            "style_tags": style_tag,
                            "area": area,
                            "provider": provider

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
                    if year:
                        temp_dic = {}
                        temp_dic["year"] = year
                        data_dic.update(temp_dic)
                    if provider:
                        temp_dic = {}
                        temp_dic["provider"] = provider
                        data_dic.update(temp_dic)
                    self.parse_data(data_dic, project_name)

            except Exception as e:
                print(e)
                # self.driver.close()

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
        # self.driver.get(releaserUrl)
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
                print(res)
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

    def video_page_api(self, task=0):
        has_data = rds_get.dbsize()
        while has_data:
            keys = rds_get.randomkey()
            res = rds_get.hgetall(keys)
            # res = rds_get.hgetall("iqiyi_100010201")
            has_data = rds_get.dbsize()
            # time.sleep(0.2)
            try:
                print(res["url"])
                self.detail_page_api(res)
                # self.detail_page_api(res)
                rds_get.delete(keys)
            except Exception as e:
                print(e, res["url"])

    def video_page(self, task=0):
        # 获取专辑页标签分类
        self.driver = webdriver.Chrome(options=self.chrome_options)
        self.driver.maximize_window()
        has_data = rds_get.dbsize()
        while has_data:
            keys = rds_get.randomkey()
            res = rds_get.hgetall(keys)
            has_data = rds_get.dbsize()
            # time.sleep(0.2)
            try:
                self.driver.get(res["url"])
                time.sleep(5)
                # self.driver.implicitly_wait(10)
                title = res["title"]
                # WebDriverWait(self.driver, 10,2).until(lambda x: x.find_element_by_xpath("/html[1]/body[1]/div[2]/div[1]/div[1]/div[1]/div[1]/div[3]/div[2]/div[1]/div[1]/div[1]/div[1]/div[1]/div[2]/div[1]/div[1]/div[1]/article[1]"))
                playcount_obj = self.driver.find_element_by_xpath("//span[@class='basic-title']")
                action = ActionChains(self.driver)
                action.click(playcount_obj).perform()
                play_conut_obj_lsit = self.driver.find_elements_by_xpath("//div[@class='hot-chart-tab']//li")
                action = ActionChains(self.driver)
                action.click(play_conut_obj_lsit[-1]).perform()
                for x in range(11):
                    # print(x)
                    ActionChains(self.driver).move_by_offset(10 * x, 20).perform()
                play_count_sum_obj = self.driver.find_element_by_xpath("//div[@class='hot-chart']//div[2]")
                print(play_count_sum_obj.location)
                print(play_count_sum_obj.text)
                print("task ", task)
                action = ActionChains(self.driver)
                # action.move_by_offset(1406,1048)
                # action.move_to_element_with_offset(play_count_sum_obj,500,-300).perform()
                # time.sleep(0.5)
                play_count_re = re.findall('指数：(\d+)', play_count_sum_obj.text)
                if play_count_re:
                    play_count = play_count_re[0]
                else:
                    play_count = ""
                print(play_count_re)
                language = self.driver.find_elements_by_xpath("//div[@class='intro-mn']//a[1]")
                if language:
                    language = language[0].text
                else:
                    language = ""
                style_tags = self.driver.find_elements_by_xpath("//div[@class='intro-mn']//a[2]")
                if style_tags:
                    style_tags = style_tags[0].text
                else:
                    style_tags = ""
                project_tags = self.driver.find_elements_by_xpath("//div[@class='intro-mn']//a[3]")
                if project_tags:
                    project_tags = project_tags[0].text
                else:
                    project_tags = ""
                # faverate_count_sum = self.driver.find_element_by_xpath("//span[@class='like-icon-box']")
                one_video_dic = {
                        "play_count_sum": play_count,
                        "language": language,
                        "style_tags": style_tags,
                        "project_tags": project_tags,
                }
                self.parse_data(one_video_dic, keys)
                # self.detail_page_api(res)
                rds_get.delete(keys)
            except Exception as e:
                print(e, res["url"])

    def one_video_page(self, title, one_video_dic, type="list"):
        video_list = self.driver.find_elements_by_xpath("//ul[@class='qy-episode-txt']//li")
        # for video_obj in video_list:
        #     title = ""
        #     video_url = ""
        #     barrage_count = ""
        #     self.parse_single_data(dic, project_name)

    def detail_page_api(self, data):
        # 获取专辑的单条视频ID
        aid = data.get("id")
        headers = {
                "Host": "iface2.iqiyi.com",
                "t": "873713668",
                "X-B3-TraceId": "2B2E20501CEF485382A9B6FD5405861D",
                "Accept-Language": "zh-cn",
                "X-B3-SpanId": "82A9B6FD5405861D",
                "Connection": "keep-alive",
                "Accept": "*/*",
                "User-Agent": "QIYIVideo/10.11.0 (iOS;com.qiyi.hd;iOS13.3;iPad6,11) Corejar",
                "sign": "a900abd6cbfb9e012dbdffa1c72f4a41",
                "Accept-Encoding": "gzip",
                "X-B3-Sampled": "0"
        }

        headers = {
                "Host": "cards.iqiyi.com",
                "t": "873713829",
                "X-B3-TraceId": "DD4CB3B31E544C02AA5BC3340AE2ABC2",
                "Accept-Language": "zh-cn",
                "X-B3-SpanId": "AA5BC3340AE2ABC2",
                "Connection": "keep-alive",
                "Accept": "*/*",
                "User-Agent": "QIYIVideo/10.11.0 (iOS;com.qiyi.hd;iOS13.3;iPad6,11) Corejar",
                "sign": "292c33bc4620256820ea5e1cdf92fcbb",
                "Accept-Encoding": "gzip",
                "X-B3-Sampled": "0",
        }
        page = 0
        has_more = True
        if has_more:
            get_dic = {
                    "api_v": "7.7",
                    "app_k": "f0f6c3ee5709615310c0f053dc9c65f2",
                    "app_v": "10.11.0",
                    "app_gv": "",
                    "app_t": "0",
                    "platform_id": "13",
                    "dev_os": "13.3",
                    "dev_ua": "iPad6,11",
                    "dev_hw": '{"cpu":"","mem":"1634"}',
                    "net_sts": "1",
                    "scrn_sts": "2",
                    "scrn_res": "2048*1536",
                    "scrn_dpi": "786432",
                    "qyid": "7AED33DD-0F97-418D-AFAA-72ED0578A75E",
                    "cupid_uid": "7AED33DD-0F97-418D-AFAA-72ED0578A75E",
                    "cupid_v": "3.37.007",
                    "psp_uid": "2345294915",
                    "psp_cki": "12Q4kR4QaQDO0H4MuUPGay47DxLxzZF7IGAqOm1Vleux5dtgo2AazHR4K5l9SLRjKQY18",
                    "secure_p": "iPad",
                    "secure_v": "1",
                    "lang": "zh_CN",
                    "app_lm": "cn",
                    "upd": "0",
                    "psp_vip": "1",
                    "core": "1",
                    "req_sn": "1577108056510",
                    "net_ip": "%7B%22country%22:%22%E4%B8%AD%E5%9B%BD%22,%22province%22:%22%E5%8C%97%E4%BA%AC%22,%22city%22:%22%E5%8C%97%E4%BA%AC%22,%22cc%22:%22%E8%81%94%E9%80%9A%22,%22area%22:%22%E5%8D%8E%E5%8C%97%22,%22timeout%22:0,%22respcode%22:0%7D",
                    "psp_status": "3",
                    "profile": '{"group":"2","counter":"1","hy_id":"","recall_firstdate":"-1","first_time":"20180813"}',
                    "gps": "",
                    "cust_count": "",
                    "idfa": "7AED33DD-0F97-418D-AFAA-72ED0578A75E",
                    "scrn_scale": "2",
                    "ouid": "3b8797b0d649aa05ece4c769a6d3d745c897ddc2",
                    "youth_model": "0",
                    "bi_params": "",
                    "from_rpage": "Y2F0ZWdvcnlfaG9tZS4z",
                    "player_rpage": "half_ply",
                    "player_block": "Ym9mYW5ncWkx",
                    "page": "player_tabs",
                    "fake_ids": "choose_set",
                    "album_id": aid,
                    # "tv_id": aid,
                    "plt_full": "1",
                    "full": "1",
                    "block_mode": "2",
                    "pps": "0",
                    "req_times": "1",

            }
            page_res = requests.get(data.get("url"), headers=headers)
            # print(page_res.text)
            # get_url = "https://iface2.iqiyi.com/views/3.0/card_view?%s" % urllib.parse.urlencode(get_dic)
            get_url = "https://cards.iqiyi.com/views_plt/3.0/card_view?api_v=7.7&app_k=f0f6c3ee5709615310c0f053dc9c65f2&app_v=10.11.0&app_gv=&app_t=0&platform_id=13&dev_os=13.3&dev_ua=iPad6,11&net_sts=1&qyid=7AED33DD-0F97-418D-AFAA-72ED0578A44E&cupid_uid=7AED33DD-0F97-418D-AFAA-72ED0578A44E&cupid_v=3.37.007&psp_uid=2345294915&psp_cki=12Q4kR4QaQDO0H4MuUPGay47DxLxzZF7IGAqOm1Vleux5dtgo2AazHR4K5l9SLRjKQY18&psp_status=3&secure_p=iPad&secure_v=1&core=1&req_sn=1577009664125&lang=zh_CN&upd=0&app_lm=cn&profile=%7B%22group%22%3A%222%22%2C%22counter%22%3A1%2C%22hy_id%22%3A%22%22%2C%22recall_firstdate%22%3A%22-1%22%2C%22first_time%22%3A%2220180813%22%7D&gps=&idfa=7AED33DD-0F97-418D-AFAA-72ED0578A44E&ouid=3b8797b0d649aa05ece4c769a6d3d745c897ddc2&scrn_scale=2&init_type=0&youth_model=0&bi_params=&wifi_mac=&layout_v=49.1&layout_name=base_layout_pad&page=player_tabs&fake_ids=spoiler_list&album_id={0}&tv_id={1}&video_tab=1&ad_play_source=0&scrn_res=2048*1536&req_times=1".format(
                    aid, aid)

            page += 1
            res = requests.get(get_url, headers=headers)
            # print(res.text)
            res_json = res.json()
            # data_list = res_json["cards"][0]["data"]
            data_list_lis = res_json["cards"][0]["blocks"]
            data_list = {}
            for k, d in enumerate(data_list_lis):
                data_list[k] = d
            for count, data_dic in enumerate(data_list):
                # vid = data_list[data_dic]["_id"]
                vid = data_list[data_dic]["block_id"]
                # description = data_list[data_dic]["other"]["_t"]
                duration = data_list[data_dic]["other"]["duration"]
                issueTime = data_list[data_dic]["other"].get("year")
                # video_title = data_list[data_dic]["meta"][0]["text"]
                video_title = data_list[data_dic]["metas"][0]["text"]
                # video_count = data_list[data_dic]["marks"]["br"]["t"]
                # print(data.get("url"))
                one_video_dic = {
                        "url": data.get("url"),
                        "vid": vid,
                        # "description": description,
                        "issueTime": issueTime,
                        "duration": duration,
                        "album": data.get("title"),
                        "video_title": video_title,
                        # "video_count":video_count
                }
                self.single_page_api(one_video_dic)

    def single_page_api(self, one_video_dic):
        # 获取单集视频url
        vid = one_video_dic["vid"]
        headers = {
                "Host": "iface2.iqiyi.com",
                "t": "938224906",
                "X-B3-TraceId": "D3E5A60235FA4EA59816008A391098B8",
                "Accept-Language": "zh-cn",
                "X-B3-SpanId": "9816008A391098B8",
                "Connection": "keep-alive",
                "Accept": "*/*",
                "User-Agent": "QIYIVideo/10.11.0 (iOS;com.qiyi.hd;iOS13.3;iPad6,11) Corejar",
                "sign": "64b4cfe0a12109fa4b011deef366fa95",
                "Accept-Encoding": "gzip",
                "X-B3-Sampled": "0",
        }
        get_head = {
                "app_k": "f0f6c3ee5709615310c0f053dc9c65f2",
                "api_v": "7.7",
                "app_v": "10.11.0",
                # "qyid": "7AED33DD-0F97-418D-AFAA-72ED0578A44E",
                # "cupid_uid": "7AED33DD-0F97-418D-AFAA-72ED0578A44E",
                "secure_p": "iPad",
                "secure_v": "1",
                "psp_uid": "2345294915",
                "psp_cki": "12Q4kR4QaQDO0H4MuUPGay47DxLxzZF7IGAqOm1Vleux5dtgo2AazHR4K5l9SLRjKQY18",
                "dev_hw": "%7B%22cpu%22:%22%22,%22mem%22:%221634%22%7D",
                "net_sts": "1",
                "scrn_sts": "2",
                "dev_os": "13.3",
                "dev_ua": "iPad6,11",
                "scrn_res": "2048*1536",
                "scrn_dpi": "786432",
                "req_sn": "1577107375313",
                "core": "1",
                "lang": "zh_CN",
                "upd": "0",
                "app_lm": "cn",
                "net_ip": "%7B%22country%22:%22%E4%B8%AD%E5%9B%BD%22,%22province%22:%22%E5%8C%97%E4%BA%AC%22,%22city%22:%22%E5%8C%97%E4%BA%AC%22,%22cc%22:%22%E8%81%94%E9%80%9A%22,%22area%22:%22%E5%8D%8E%E5%8C%97%22,%22timeout%22:0,%22respcode%22:0%7D",
                "cupid_v": "3.37.007",
                "youth_model": "0",
                "bi_params": "",
                "album_id": vid,
                "tv_id": vid,
                "play_res": "512",
                "play_retry": "0",
                "play_core": "1",
                "plugin_id": "",
                "platform_id": "13",
                "app_p": "ipad",
                "app_t": "0",
                "ctl_dubi": "0",
                "req_sn": "1577107375313240000",
                "req_times": "1",
                "scrn_scale": "2",
                "rate": "512,16,8,1024,2048",
                "rpage": "half_ply",
                "block": "bofangqi1",
                "s2": "player_tabs",
                "uid": "2345294915",
                "version": "10.11.0",
                "pps": "0",

        }
        try:
            res = requests.get("https://iface2.iqiyi.com/video/3.0/v_play?%s" % urllib.parse.urlencode(get_head),
                               headers=headers)
            res_json = res.json()
            if_pay = res_json["video"]["pay_mark"]
            glb_year = res_json["video"]["glb_year"]
            video_rul = res_json["video"]["web_url"]
            description = res_json["video"]["subtitle"]
            project_tags = res_json["album"]["tag"]
            video_title = res_json["album"]["_t"]
            year = res_json["album"]["year"]
            one_video_dic["if_pay"] = if_pay
            one_video_dic["year"] = glb_year
            one_video_dic["video_rul"] = video_rul
            one_video_dic["description"] = description
            one_video_dic["project_tags"] = project_tags
            one_video_dic["video_title"] = video_title
            one_video_dic["year"] = year
            self.parse_single_data(one_video_dic, video_rul)
        except Exception as e:
            print(e)
            pass

    def video_page_seleium(self, task=0):
        # seleium 获取单集视频 缺少vid
        self.driver = webdriver.Chrome(options=self.chrome_options)
        self.driver.maximize_window()
        has_data = rds_get.dbsize()
        while has_data:
            keys = rds_get.randomkey()
            res = rds_get.hgetall(keys)
            has_data = rds_get.dbsize()
            # time.sleep(0.2)
            try:
                self.driver.get(res["url"])
                time.sleep(5)
                video_list = self.driver.find_elements_by_xpath("//div[@id='rightPlayList']//li")
                for count, video_obj in enumerate(video_list):
                    # if count <= 1:
                    #     continue
                    try:
                        ActionChains(self.driver).click(video_obj).perform()
                        time.sleep(2)
                    except:
                        continue
                    self.driver.implicitly_wait(10)
                    title = self.driver.find_element_by_xpath("//h1[@class='player-title']").text
                    # WebDriverWait(self.driver, 10,2).until(lambda x: x.find_element_by_xpath("/html[1]/body[1]/div[2]/div[1]/div[1]/div[1]/div[1]/div[3]/div[2]/div[1]/div[1]/div[1]/div[1]/div[1]/div[2]/div[1]/div[1]/div[1]/article[1]"))
                    # playcount_obj = self.driver.find_element_by_xpath("//span[@class='basic-title']")
                    duration_str = self.driver.find_element_by_xpath("//iqpspan[@class='iqp-time-dur']").text
                    duration = trans_duration(duration_str)
                    # action = ActionChains(self.driver)
                    # action.click(playcount_obj).perform()
                    # play_conut_obj_lsit = self.driver.find_elements_by_xpath("//div[@class='hot-chart-tab']//li")
                    # action = ActionChains(self.driver)
                    # action.click(play_conut_obj_lsit[-1]).perform()
                    # for x in range(11):
                    #     # print(x)
                    #     ActionChains(self.driver).move_by_offset(10 * x, 20).perform()
                    # play_count_sum_obj = self.driver.find_element_by_xpath("//div[@class='hot-chart']//div[2]")
                    # print(play_count_sum_obj.location)
                    # print(play_count_sum_obj.text)
                    print("task ", task)
                    action = ActionChains(self.driver)
                    # action.move_by_offset(1406,1048)
                    # action.move_to_element_with_offset(play_count_sum_obj,500,-300).perform()
                    # time.sleep(0.5)
                    # play_count_re = re.findall('指数：(\d+)', play_count_sum_obj.text)
                    # if play_count_re:
                    #     play_count = play_count_re[0]
                    # else:
                    #     play_count = ""
                    # print(play_count_re)
                    # language = self.driver.find_elements_by_xpath("//div[@class='intro-mn']//a[1]")
                    # if language:
                    #     language = language[0].text
                    # else:
                    #     language = ""
                    # style_tags = self.driver.find_elements_by_xpath("//div[@class='intro-mn']//a[2]")
                    # if style_tags:
                    #     style_tags = style_tags[0].text
                    # else:
                    #     style_tags = ""
                    # project_tags = self.driver.find_elements_by_xpath("//div[@class='intro-mn']//a[3]")
                    # if project_tags:
                    #     project_tags = project_tags[0].text
                    # else:
                    #     project_tags = ""
                    # faverate_count_sum = self.driver.find_element_by_xpath("//span[@class='like-icon-box']")
                    one_video_dic = {
                            # "play_count_sum": play_count,
                            "url": res["url"],
                            "video_url": self.driver.current_url,
                            "video_title": title,
                            "album": res["title"],
                            "duration": duration
                    }
                    self.parse_single_data(one_video_dic, one_video_dic["video_url"])
                # self.detail_page_api(res)
                rds_get.delete(keys)
            except Exception as e:
                print(e, res["url"])

    def comment_like_api(self,task=0):
        # 获取评论和点赞
        has_data = rds_get.dbsize()
        while has_data:
            keys = rds_get.randomkey()
            res = rds_get.hgetall(keys)
            has_data = rds_get.dbsize()

            try:
                vid = res.get("vid")
                if not vid:
                    rds_get.delete(keys)
                comment_headers = {
                        "Host": "cards.iqiyi.com",
                        "t": "938224906",
                        "X-B3-TraceId": "4AF7FFCBC6A9460CB396CC59D1754D3B",
                        "Accept-Language": "zh-cn",
                        "X-B3-SpanId": "B396CC59D1754D3B",
                        "Connection": "keep-alive",
                        "Accept": "*/*",
                        "User-Agent": "QIYIVideo/10.11.0 (iOS;com.qiyi.hd;iOS13.3;iPad6,11) Corejar",
                        "sign": "64b4cfe0a12109fa4b011deef366fa95",
                        "Accept-Encoding": "gzip",
                        "X-B3-Sampled": "0",

                }
                like_headers = {
                        "Host": "iface2.iqiyi.com",
                        "t": "938306077",
                        "X-B3-TraceId": "7FABCAD44B0249AE85A86B71BCE6DFDB",
                        "Accept-Language": "zh-cn",
                        "X-B3-SpanId": "85A86B71BCE6DFDB",
                        "Connection": "keep-alive",
                        "Accept": "*/*",
                        "User-Agent": "QIYIVideo/10.11.0 (iOS;com.qiyi.hd;iOS13.3;iPad6,11) Corejar",
                        "sign": "6fd76e1f02b7817dfd32ae5591d8b987",
                        "Accept-Encoding": "gzip",
                        "X-B3-Sampled": "0",
                }

                comment_url = "https://cards.iqiyi.com/views_sns/3.0/short_video_comments?app_gv=&core=1&from_subtype=-1&upd=0&profile=%257B%2522group%2522%253A%25222%2522%252C%2522counter%2522%253A2%252C%2522hy_id%2522%253A%2522%2522%252C%2522recall_firstdate%2522%253A%2522-1%2522%252C%2522first_time%2522%253A%252220180813%2522%257D&app_lm=cn&secure_p=iPad&lang=zh_CN&cupid_v=3.37.007&scrn_scale=2&gps=&phone_operator=0&dev_os=13.3&ouid=3b8797b0d649aa05ece4c769a6d3d745c897ddc2&layout_v=49.1&content_uid=0&psp_cki=12Q4kR4QaQDO0H4MuUPGay47DxLxzZF7IGAqOm1Vleux5dtgo2AazHR4K5l9SLRjKQY18&card_v=3.0&youth_model=0&page_st=common&app_k=f0f6c3ee5709615310c0f053dc9c65f2&dev_ua=iPad6,11&net_sts=1&cupid_uid=7AED33DD-0F97-418D-AFAA-72ED0578A64E&from_type=-1&init_type=0&app_v=10.11.0&idfa=7AED33DD-0F97-418D-AFAA-72ED0578A64E&app_t=0&platform_id=13&content_id={0}&layout_name=base_layout_pad&req_sn=1577107375553&api_v=7.7&nav_search_v2=0&psp_status=3&psp_uid=2345294915&wifi_mac=&qyid=7AED33DD-0F97-418D-AFAA-72ED0578A64E&wall_id=0&bi_params=&secure_v=1&content_pic=http://pic7.iqiyipic.com/image/20180103/bc/6f/v_114479444_m_601_m1.jpg&identity=1&albumid={1}&req_times=1".format(
                    vid, vid)
                like_url = "https://iface2.iqiyi.com/views/3.0/card_view?api_v=7.7&app_k=f0f6c3ee5709615310c0f053dc9c65f2&app_v=10.11.0&app_gv=&app_t=0&platform_id=13&dev_os=13.3&dev_ua=iPad6,11&dev_hw=%7B%22cpu%22:%22%22,%22mem%22:%221634%22%7D&net_sts=1&scrn_sts=2&scrn_res=2048*1536&scrn_dpi=786432&qyid=7AED33DD-0F97-418D-AFAA-72ED0578A54E&cupid_uid=7AED33DD-0F97-418D-AFAA-72ED0578A54E&cupid_v=3.37.007&psp_uid=2345294915&psp_cki=12Q4kR4QaQDO0H4MuUPGay47DxLxzZF7IGAqOm1Vleux5dtgo2AazHR4K5l9SLRjKQY18&secure_p=iPad&secure_v=1&lang=zh_CN&app_lm=cn&upd=0&psp_vip=1&core=1&req_sn=1577187512020&net_ip=%7B%22country%22:%22%E4%B8%AD%E5%9B%BD%22,%22province%22:%22%E5%8C%97%E4%BA%AC%22,%22city%22:%22%E5%8C%97%E4%BA%AC%22,%22cc%22:%22%E8%81%94%E9%80%9A%22,%22area%22:%22%E5%8D%8E%E5%8C%97%22,%22timeout%22:0,%22respcode%22:0%7D&psp_status=3&profile=%257B%2522group%2522%253A%25222%2522%252C%2522counter%2522%253A1%252C%2522hy_id%2522%253A%2522%2522%252C%2522recall_firstdate%2522%253A%2522-1%2522%252C%2522first_time%2522%253A%252220180813%2522%257D&gps=&cust_count=&idfa=7AED33DD-0F97-418D-AFAA-72ED0578A54E&scrn_scale=2&ouid=3b8797b0d649aa05ece4c769a6d3d745c897ddc2&youth_model=0&bi_params=&from_rpage=Y2F0ZWdvcnlfaG9tZS4z&player_rpage=half_ply&player_block=Ym9mYW5ncWkx&page=player_tabs&fake_ids=choose_set&album_id={0}&tv_id={1}&plt_full=1&full=1&block_mode=2&pps=0&req_times=1".format(
                    vid, vid)
                comment_res = requests.get(comment_url, headers=comment_headers)
                like_res = requests.get(like_url, headers=like_headers)
                comment_res_json = comment_res.json()
                like_res_json = like_res.json()
                comment_count = comment_res_json["kv_pair"]["totalCount"]
                try:
                    favorite_count = like_res_json["kvpairs"]["likeCount"]
                except:
                    favorite_count = " "
                dic = {
                        "comment_count":comment_count,
                        "favorite_count":favorite_count
                }
                print(dic,task)
                # print(res)
                self.parse_single_data(dic,keys)
                rds_get.delete(keys)
            except Exception as e:
                print(e)
                continue

    def get_vid(self):
        has_data = rds_get.dbsize()
        while has_data:
            keys = rds_get.randomkey()
            res = rds_get.hgetall(keys)
            has_data = rds_get.dbsize()
            vid = res.get("vid")

            if not vid:
                print(vid,keys)
                headers = {

                        "accept": "*/*",
"accept-encoding": "gzip, deflate, br",
"accept-language": "zh,zh-CN;q=0.9",
"cookie": 'QP001=1; QP0017=100; QP0018=100; QC005=c48ede3bf6d027f313486d3475d317b7; QC006=d6c2a542ed01ba7953ae094d36a40083; T00404=c8c154e05c0e54572a5eb20047dd6129; QC173=0; Hm_lvt_53b7374a63c37483e5dd97d78d9bb36e=1574822745,1575886435,1576549484; QC007=DIRECT; QC008=1556242615.1576549484.1576549484.9; nu=0; P00004=.1576737946.c60f13e3da; P00001=87BSsO9M7Lkm1M5cGXEBptdVHC3f39m3vCo3kPoLxm1PbUJm1dlpDUm21zbVGaUrgcWkPNy0e; P00003=2345294915; P00010=2345294915; P00007=87BSsO9M7Lkm1M5cGXEBptdVHC3f39m3vCo3kPoLxm1PbUJm1dlpDUm21zbVGaUrgcWkPNy0e; P00PRU=2345294915; P000email=litaolemo%40foxmail.com; QC160=%7B%22u%22%3A%2215652340574%22%2C%22lang%22%3A%22%22%2C%22local%22%3A%7B%22name%22%3A%22%E4%B8%AD%E5%9B%BD%E5%A4%A7%E9%99%86%22%2C%22init%22%3A%22Z%22%2C%22rcode%22%3A48%2C%22acode%22%3A%2286%22%7D%2C%22type%22%3A%22p1%22%7D; QC170=1; QC175=%7B%22upd%22%3Atrue%2C%22ct%22%3A1576737959108%7D; QP0013=1; QP007=0; QC179=%7B%22userIcon%22%3A%22//www.iqiyipic.com/common/fix/headicons/male-130.png%22%2C%22vipTypes%22%3A%221%22%7D; QY_PUSHMSG_ID=c48ede3bf6d027f313486d3475d317b7; P00002=%7B%22uid%22%3A2345294915%2C%22pru%22%3A2345294915%2C%22user_name%22%3A%2215652340574%22%2C%22nickname%22%3A%22litaolemo001%22%2C%22pnickname%22%3A%22litaolemo001%22%2C%22type%22%3A11%2C%22email%22%3A%22litaolemo%40foxmail.com%22%7D; QC163=1; QC001=1; c242345294915=1576742527454; TQC022=%7B%22au%22%3A%2287BSsO9M7Lkm1M5cGXEBptdVHC3f39m3vCo3kPoLxm1PbUJm1dlpDUm21zbVGaUrgcWkPNy0e%22%2C%22ak%22%3A%228ffff92ffm33w8CqFm13T6zA4tKtsnjA5rL2CEa4Qsva91f6xsPMsQm4%22%7D; PCAU=0; QYABEX={"pcw_home_movie":{"value":"new","abtest":"146_B"}}; T00700=EgcI77-tIRAFEgcIz7-tIRAB; QY00001=2345294915; H_RENEW_COUNT=1576810189038; IQIYIH5_WEBP=1; P01010=1576857600; ut={%22ut%22:[%221%22]%2C%22uid%22:2345294915}; QILINPUSH=1; QC176=%7B%22state%22%3A0%2C%22ct%22%3A1576811187124%7D; Hm_lpvt_53b7374a63c37483e5dd97d78d9bb36e=1576811226; QC159=%7B%22color%22%3A%22FFFFFF%22%2C%22channelConfig%22%3A1%2C%22isOpen%22%3A1%2C%22speed%22%3A13%2C%22density%22%3A30%2C%22opacity%22%3A86%2C%22isFilterColorFont%22%3A1%2C%22proofShield%22%3A0%2C%22forcedFontSize%22%3A24%2C%22isFilterImage%22%3A1%2C%22hadTip%22%3A1%7D; app_server_fail_num=2; app_server_retry_time=1; QP008=3000; IMS=IggQBhj_3_PvBSokCiA3ZjNlMjJmMjUxYzkzYTY1N2I2ZGZlMmFiMGQzOTY1YRAA; QC010=214521235; __dfp=a0c95636e67e384d24a08f486f2e6af20530557a862431f4441f2f23083da385e8@1577845483549@1576549484549',
"referer": "https://www.iqiyi.com/a_19rriflrba.html",
"sec-fetch-mode": "no-cors",
"sec-fetch-site": "same-site",
"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36",

                }
                requests_re = requests.get(res.get("video_url"),headers=headers)
                try:
                    vid = re.findall("tvid=(\d+)",requests_re.text)[0]
                    if not vid:
                        continue
                    dic = {
                            "vid":vid
                    }
                    self.parse_single_data(dic, keys)
                    print(1)
                except:
                    continue
if __name__ == "__main__":
    # crlawler_iqiyi = CrawlerMain()
    # provider_dic = {
    #     # "ALL":"http://list.iqiyi.com/www/3/-------------24-1-1-iqiyi--.html"
    #         "人物":70,
    #         "军事":72,
    #         "历史":74,
    #         "探索":73,
    #         "文化":77,
    #         "社会":71,
    #         "科学":28119,
    #         "旅游":310,
    #         "真人秀":28137,
    #         "政治":28138,
    # }
    # for provider in provider_dic:
    #     crlawler_iqiyi.list_page(releaserUrl=provider_dic[provider],
    #                              video_list_xpath="/html[1]/body[1]/div[6]/div[1]/div[2]/div",
    #                              roll=1000,
    #                              project_tag=provider,
    #                              category_id=str(provider_dic[provider])
    #                              # provider=provider
    #                              )
    # provider_dic = {
    #         # "ALL":"http://list.iqiyi.com/www/3/-------------24-1-1-iqiyi--.html"
    #         "BBC": 28468,
    #         "美国历史频道": 28470,
    #         "探索频道": 28471,
    #         "央视记录": 28472,
    #         "北京纪实频道": 28473,
    #         "上海纪实频道": 28474,
    #         "朗思文化": 28476,
    #         "CNEX": 28477,
    #         "五星传奇": 28478,
    #         "IMG": 28479,
    #         "NHK": 28480,
    #         "爱奇艺出品": 31283,
    #         "Netflix": 31286,
    # }
    # for provider in provider_dic:
    #     crlawler_iqiyi.list_page(releaserUrl=provider_dic[provider],
    #                              video_list_xpath="/html[1]/body[1]/div[6]/div[1]/div[2]/div",
    #                              roll=1000,
    #                              provider=provider,
    #                              category_id=str(provider_dic[provider])
    #                              # provider=provider
    #                              )
    # provider_dic = {
    #         # "ALL":"http://list.iqiyi.com/www/3/-------------24-1-1-iqiyi--.html"
    #         "纪录电影": 29077,
    #         "系列纪录片": 29078,
    #         "网络纪录片": 29082,
    #         "纪实栏目": 29083,
    #
    # }
    # for provider in provider_dic:
    #     crlawler_iqiyi.list_page(releaserUrl=provider_dic[provider],
    #                              video_list_xpath="/html[1]/body[1]/div[6]/div[1]/div[2]/div",
    #                              roll=1000,
    #                              style_tags=provider,
    #                              category_id=str(provider_dic[provider])
    #                              # provider=provider
    #                              )
    # executor = ProcessPoolExecutor(max_workers=4)
    # futures = []
    # # crlawler_tecnt = CrawlerMain()
    # # process_task = crlawler_bilbili.video_page
    # for one_scan in range(4):
    #     crlawler_tecnt = CrawlerMain()
    #     future = executor.submit(crlawler_tecnt.video_page, task=one_scan)
    #     futures.append(future)
    #
    #     # crlawler_bilbili.video_page(one_scan)
    # executor.shutdown(True)
    # crlawler_iqiyi = CrawlerMain()
    # crlawler_iqiyi.video_page()
    # revise_data()

    # executor = ProcessPoolExecutor(max_workers=4)
    # futures = []
    # for one_scan in range(4):
    #     crlawler_tecnt = CrawlerMain()
    #     future = executor.submit(crlawler_tecnt.video_page_api, task=one_scan)
    #     futures.append(future)
    #
    #     # crlawler_bilbili.video_page(one_scan)
    # executor.shutdown(True)
    # crlawler_iqiyi = CrawlerMain()
    # crlawler_iqiyi.video_page_api()

    # crlawler_iqiyi = CrawlerMain()
    # crlawler_iqiyi.get_vid()
    # crlawler_iqiyi.video_page_seleium()


    # executor = ProcessPoolExecutor(max_workers=4)
    # futures = []
    # for one_scan in range(4):
    #     crlawler_tecnt = CrawlerMain()
    #     future = executor.submit(crlawler_tecnt.comment_like_api, task=one_scan)
    #     futures.append(future)
    # # crlawler_bilbili.video_page(one_scan)
    # executor.shutdown(True)
    crlawler_iqiyi = CrawlerMain()
    crlawler_iqiyi.comment_like_api()
