# -*- coding:utf-8 -*-
# @Time : 2019/12/27 16:48 
# @Author : litao


# -*- coding:utf-8 -*-
# @Time : 2019/12/27 15:49
# @Author : litao
import numpy as np
import random
import argparse
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
from lxml import etree
from crawler.crawler_sys.proxy_pool.func_get_proxy_form_kuaidaili import get_proxy
from bs4 import BeautifulSoup

# rds_list = redis.StrictRedis(host='127.0.0.1', port=6379, db=1, decode_responses=True)
# rds_single = redis.StrictRedis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
# rds_get = redis.StrictRedis(host='127.0.0.1', port=6379, db=15, decode_responses=True)

# rds_copy = redis.StrictRedis(host='127.0.0.1', port=6379, db=1, decode_responses=True)
rds_list = redis.StrictRedis(host='192.168.17.60', port=6379, db=0, decode_responses=True)
rds_single = redis.StrictRedis(host='192.168.17.60', port=6379, db=0, decode_responses=True)
rds_get = redis.StrictRedis(host='192.168.17.60', port=6379, db=12, decode_responses=True)

parser = argparse.ArgumentParser(description='Specify a platform name.')

parser.add_argument('-p', '--max_page', default=0, type=int,
                    help=('The max page numbers'))
parser.add_argument('-t', '--style_tag', default="", type=str,
                    help=('style_tag'))
parser.add_argument('-c', '--countries', default="", type=str,
                    help=('style_tag'))
args = parser.parse_args()


def get_url():
    cat_id_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 100]
    sourceId_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 100]
    year_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
    sort_list = [1, 2, 3]
    for cat in cat_id_list:
        for source in sourceId_list:
            for year in year_list:
                for sort in sort_list:
                    if cat < 5:
                        continue
                    else:
                        if cat <= 5 and source < 14:
                            continue

                    yield cat,source,year,sort

def revise_data():
    scan_re = rds_list.scan_iter()
    for one_scan in scan_re:
        # print(one_scan)
        data = rds_list.hgetall(one_scan)
        # data["title"] = data["title"].replace("\r", "").replace("\n", "")
        # data["describe"] = data["describe"].replace("\r", "").replace("\n", "")
        if not data.get("directors"):
            rds_get.hmset(one_scan, data)
        # rds_list.hmset(one_scan,data)


class Crawler_main(object):
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
                "platform": "douban",
                "ID": "",
                "title": "",
                "url": "",
                "directors": "",
                "screenwriter": "",
                "casts": "",
                "describe": "",
                "year": "",
                "provider": "",
                "style_tags": "",
                "project_tags": "",
                "language": "",
                "area": "",
                "rate": "",
                "comment_count": ""

        }

    def __exit__(self):
        self.driver.close()

    def list_page(self, releaserUrl=None,
                  video_list_xpath=None,
                  play_count_xpath=None,
                  if_free=None,
                  title=None,
                  describe=None,
                  project_tag="",
                  provider=None,
                  year=None,
                  next_page_xpath=None,
                  roll=None,
                  style_tags=None,
                  countries=""
                  ):
        offset = 30
        headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh,zh-CN;q=0.9",
                "Connection": "keep-alive",
                # "Cookie": '__mta=150368905.1577424190198.1577424190198.1577433956085.2; uuid_n_v=v1; uuid=F6EC1BC0286811EAA13C754DA9FC705E01959D18445546A1A0F7A8FE8311D8BD; _csrf=c8be65f46b2c830502aa6a49c2f1aacb1660ffb3e3a6c4ae3623084677b66d7c; Hm_lvt_703e94591e87be68cc8da0da7cbd0be2=1577424189; _lxsdk_cuid=16f45cef868c8-03522a76dfe0d8-5f4e2917-161012-16f45cef868c8; _lxsdk=F6EC1BC0286811EAA13C754DA9FC705E01959D18445546A1A0F7A8FE8311D8BD; mojo-uuid=396ea3294dbf9178fa564b08543aed72; mojo-session-id={"id":"f35010c2739ba6f036e332417fe21f84","time":1577433601641}; Hm_lpvt_703e94591e87be68cc8da0da7cbd0be2=1577434032; __mta=150368905.1577424190198.1577433956085.1577434032548.3; mojo-trace-id=57; _lxsdk_s=16f465e962c-545-9da-13a%7C%7C64',
                "Cookie": '__mta=150368905.1577424190198.1577931921073.1577933054583.8; uuid_n_v=v1; uuid=F6EC1BC0286811EAA13C754DA9FC705E01959D18445546A1A0F7A8FE8311D8BD; _csrf=c8be65f46b2c830502aa6a49c2f1aacb1660ffb3e3a6c4ae3623084677b66d7c; _lxsdk_cuid=16f45cef868c8-03522a76dfe0d8-5f4e2917-161012-16f45cef868c8; _lxsdk=F6EC1BC0286811EAA13C754DA9FC705E01959D18445546A1A0F7A8FE8311D8BD; mojo-uuid=396ea3294dbf9178fa564b08543aed72; lt=dwim2AyVn0Nr4tMQ1qCHf87HvVwAAAAAsQkAAGKVo4UF5isSHZyJ2F-6Yypd0YqL-FIGGMTWixcuMN23AhelN_OPNDA2hAk5IuCtNg; lt.sig=0AWWI8aMHZfmuLzGDO9hoKoZqT8; Hm_lvt_703e94591e87be68cc8da0da7cbd0be2=1577424189,1577683110; mojo-session-id={"id":"8d8eb79ab4cbaf8082e721ba64b73f3a","time":1577935255982}; mojo-trace-id=1; Hm_lpvt_703e94591e87be68cc8da0da7cbd0be2=1577935256; __mta=150368905.1577424190198.1577933054583.1577935256193.9; _lxsdk_s=16f64452341-fac-102-6a1%7C265018624%7C3',
                "Host": "maoyan.com",
                "Referer": "https://maoyan.com/films?showType=3&offset=30",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.87 Safari/537.36",
        }
        count_false = 0
        if args.max_page:
            offset = args.max_page
        while True:
            try:
                time.sleep(0.5)
                print("page ", offset)
                url = "https://maoyan.com/films?showType=3&offset={0}".format(
                        str(offset))
                proxies = get_proxy(4)
                requests_res = requests.get(url, headers=headers, proxies=proxies, allow_redirects=False)
                html = etree.HTML(requests_res.text)
                # soup = BeautifulSoup(requests_res.text, 'lxml')
                # print(soup)
                # soup.contents
                dev_list = html.xpath("//body//dd")
                for dev in dev_list:
                    url_list = dev.xpath("./div[2]//a[1]/@href")
                    url = "https://maoyan.com%s" % url_list[0]
                    title = dev.xpath("./div[2]//a[1]/text()")[0]
                    rate_list = dev.xpath("./div[3]//text()")
                    rate_str = ""
                    for rate in rate_list:
                        rate_str += rate
                    data_dic = {
                            "url": url,
                            "title": title,
                            "rate": rate_str,
                    }

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
                    self.parse_data(data_dic, url, new_data=True)
                offset += 30
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

    def parse_data(self, data_dic, project_name, new_data=False):
        res = rds_list.hgetall(project_name)
        if new_data and res:
            return True
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
                if type(data_dic[k]) == list:
                    temp_str = ""
                    for d in data_dic[k]:
                        temp_str += d + ","
                    data_dic[k] = temp_str
                if type(data_dic[k]) == bool:
                    if data_dic[k]:
                        data_dic[k] = "ture"
                    else:
                        data_dic[k] = "false"
            # res.update(data_dic)
            # print(res)
            rds_list.hmset(project_name, data_dic)
        else:
            data = copy.deepcopy(self.one_video_dic)
            for k in data_dic:
                if not data_dic[k]:
                    data_dic[k] = ""
                if type(data_dic[k]) == list:
                    temp_str = ""
                    for d in data_dic[k]:
                        temp_str += d + ","
                    data_dic[k] = temp_str
                if type(data_dic[k]) == bool:
                    if data_dic[k]:
                        data_dic[k] = "ture"
                    else:
                        data_dic[k] = "false"
            # data.update(data_dic)
            # print(data)
            rds_list.hmset(project_name, data_dic)
            rds_get.hmset(project_name, data_dic)

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
                page_data = requests.get(res["url"], headers=headers, timeout=5)
            except:
                continue
            # print(page_data.status_code)
            if page_data.status_code != 200:
                rds_get.delete(keys)
                continue
            res_text = page_data.content.decode("utf-8")
            # print(page_data.content.decode("utf-8"))
            print("task %s" % task, res["url"])

            try:
                cover_info = re.findall("var COVER_INFO = (.*)\n", res_text)[0]
                cover_info_dic = json.loads(cover_info, encoding="utf-8")
            except:
                cover_info_dic = {}
            try:
                VIDEO_INFO = re.findall("var VIDEO_INFO = (.*)\n", res_text)[0]
                video_info_dic = json.loads(VIDEO_INFO, encoding="utf-8")
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
                page_data = requests.get(single_data_url, headers=headers, timeout=5)
                if page_data.status_code != 200:
                    return None
                VIDEO_INFO = re.findall("var VIDEO_INFO = (.*)\n", page_data.content.decode("utf-8"))[0]
                video_info_dic = json.loads(VIDEO_INFO, encoding="utf-8")
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

    def list_page_api(self, cat, source, year, sort,
                      releaserUrl=None,
                      video_list_xpath=None,
                      play_count_xpath=None,
                      if_free=None,
                      title=None,
                      describe=None,
                      project_tag="",
                      provider=None,
                      next_page_xpath=None,
                      roll=None,
                      style_tags=None,
                      countries=""

                      ):
        offset = 0
        headers = {
                "Host": "api.maoyan.com",
                "Connection": "Keep-Alive",
                "Accept-Encoding": "gzip",
                "User-Agent": "AiMovie /Oneplus-6.0.1-oneplus a5010-0x0-0-null-0-000000000000000-null",
                "mtgdid": "AAAAAAAAAAAAACh9V5sO1zmQc71i5gjpKNuww8T-JnDVTQHuVQFINVu2yYO8FhnCWl_Cqj2TMCWI983qEk_Ha5ayk_tXytbMWi4",
        }
        count_false = 0
        print(cat, source, year, sort)
        if args.max_page:
            offset = args.max_page
        if True:
            offset = 0
            while offset <= 2000:
                try:
                    time.sleep(0.1)
                    print("page ", offset)
                    url = "http://api.maoyan.com/mmdb/search/movie/tag/list.json?cityId=1&limit=100&offset={0}&catId={1}&sourceId={2}&yearId={3}&sortId={4}&token=7SJTJRCOW4fNMlp_xZDfgeI8qL0AAAAAsAkAADq-Y4OtjaaVeiysSdZtMsWTuGb0liEIqBPrkrC5QNJ0xOlFWRhf__Rj4D5cDS9L9g&utm_campaign=AmovieBmovieCD-1&movieBundleVersion=8012031&utm_source=meituan&utm_medium=android&utm_term=8.12.3&utm_content=440000000189785&ci=1&net=1&dModel=oneplus%20a5010&uuid=0000000000000A10631E76CD844099D6694316F7616BBA157797426456628307&channelId=1&lat=0.0&lng=0.0&refer=c_boybi6x4&version_name=8.12.3&machine_type=0".format(
                            str(offset),cat,source,year,sort)
                    proxies = get_proxy(4)
                    requests_res = requests.get(url, headers=headers, proxies=proxies, allow_redirects=False)
                    dev_list = requests_res.json()
                    for dev in dev_list["list"]:
                        data_dic = copy.deepcopy(dev)
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
                        self.parse_data(data_dic, str(data_dic["id"]), new_data=True)
                    offset += 100
                    if offset >= 2000:
                        break
                except Exception as e:
                    print(e)
                    # self.driver.close()

    def detail_page(self, task=0):
        has_data = rds_get.dbsize()
        while True:
            try:
                if not has_data:
                    time.sleep(5)
                    has_data = rds_get.dbsize()
                    continue
                keys = rds_get.randomkey()
                print(task, keys)
                res = rds_get.hgetall(keys)
                has_data = rds_get.dbsize()
                time.sleep(0.2)
                try:
                    if int(res["rt"][:4]) < 2010:
                        dic = {
                                "box_office": "",
                                "url": "https://maoyan.com/films/%s" % keys
                        }
                        self.parse_data(dic, keys)
                        rds_get.delete(keys)
                        continue
                except:
                    pass
                headers = {

"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
"Accept-Encoding": "gzip, deflate",
"Accept-Language": "zh,zh-CN;q=0.9",
"Connection": "keep-alive",
# "Cookie": "_lxsdk_cuid=16f45cef868c8-03522a76dfe0d8-5f4e2917-161012-16f45cef868c8; _lxsdk=F6EC1BC0286811EAA13C754DA9FC705E01959D18445546A1A0F7A8FE8311D8BD; Hm_lvt_703e94591e87be68cc8da0da7cbd0be2=1577424189,1577683110,1577942292; _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic; __utma=17099173.1331545914.1577942309.1577942309.1577942309.1; __utmc=17099173; __utmz=17099173.1577942309.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __mta=150368905.1577424190198.1578028660590.1578044222257.17; _lxsdk_s=16f6ac3e790-0de-cab-b27%7C265018624%7C6; uuid_n_v=v1; iuuid=9C5AAEF02E0E11EAB981AB68C7AB1D51622E552FC52545AE9F3D31A0EE1F6A4F; webp=true; selectci=; ci=1%2C%E5%8C%97%E4%BA%AC; theme=maoyan; _last_page=undefined; latlng=39.908589%2C116.397316%2C1578045092790; Hm_lpvt_703e94591e87be68cc8da0da7cbd0be2=1578045104",
"Host": "m.maoyan.com",
"Upgrade-Insecure-Requests": "1",
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.87 Safari/537.36",

                }
                # keys = 1249366
                proxies = get_proxy(4)
                url = "http://m.maoyan.com/movie/{0}/box?_v_=yes&utm_campaign=AmovieBmovieD100&f=android&userid={1}".format(
                        keys,random.randint(265011000,265031000))
                page_source = requests.get(url, headers=headers, proxies=proxies, timeout=5,allow_redirects=False)
                # print(page_source.text)
                try:
                    page_json = re.findall('AppData = (.*?);</script>', page_source.text)[0]
                    res_json = json.loads(page_json, encoding="utf-8")
                except:
                    rds_get.delete(keys)
                    continue
                    # print(page_json)
                # name = res_json.get("name")
                box_office = res_json.get("summary").get("mbox").get("sumBox")
                dic = {
                        "box_office": box_office,
                        "url":"https://maoyan.com/films/%s" % keys
                }
                print(dic)
                self.parse_data(dic, keys)
                rds_get.delete(keys)
            except Exception as e:
                # print(e)
                pass


if __name__ == "__main__":
    if args.style_tag or args.countries:
        Crawler_douban = Crawler_main()
        Crawler_douban.list_page(style_tags=args.style_tag,countries=args.countries)
    else:
        executor = ProcessPoolExecutor(max_workers=8)
        futures = []
        for one_scan in range(8):
            Crawler_douban = Crawler_main()
            future = executor.submit(Crawler_douban.detail_page, task=one_scan)
            futures.append(future)
        executor.shutdown(True)
    # executor = ProcessPoolExecutor(max_workers=8)
    # futures = []
    # for cat,source,year,sort in get_url():
    #     # print(cat,source,year,sort )
    #     Crawler_douban = Crawler_main()
    #     future = executor.submit(Crawler_douban.list_page_api, cat,source,year,sort)
    #     futures.append(future)
    # executor.shutdown(True)
    # Crawler_douban = Crawler_main()
    # Crawler_douban.list_page_api()
    # Crawler_douban.detail_page()
    # revise_data()
