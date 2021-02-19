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


rds_list = redis.StrictRedis(host='192.168.17.60', port=6379, db=1, decode_responses=True)
rds_single = redis.StrictRedis(host='192.168.17.60', port=6379, db=0, decode_responses=True)
# rds_get = redis.StrictRedis(host='192.168.17.60', port=6379, db=15, decode_responses=True)

class CrawlerMain(object):
    def __init__(self):
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--disable-gpu')
        # self.chrome_options.add_argument("--start-maximized")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        self.timestamp = str(datetime.datetime.now().timestamp() * 1e3)
        prefs = {"profile.managed_default_content_settings.images": 2}
        self.chrome_options.add_experimental_option("prefs", prefs)
        # self.driver = webdriver.Chrome(options=self.chrome_options)
        self.one_video_dic = {
                "platform": "bilibili",
                "ID": "",
                "title": "",
                "url": "",
                "describe": "",
                "video_count": "",
                "sum_duration": "",
                "year": "",
                "provider": "",
                "style_tags": "",
                "project_tags":"",
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
                "platform": "bilibili",
                "vid": "",
                "title": "",
                "video_title": "",
                "url": "",
                "video_url":"",
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


    def list_page(self, url=None,
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

        # js = "var q=document.documentElement.scrollTop=%s" % roll
        # self.driver.execute_script(js)
        while True:
            # self.driver.get(url)
            # self.driver.implicitly_wait(5)
            time.sleep(0.2)
            # self.driver.execute_script(js)
            try:
                next_page_obj = self.driver.find_elements_by_xpath(next_page_xpath)
            except:
                # self.driver.get(self.driver.current_url)
                continue
            vidoe_list_obj = self.driver.find_elements_by_xpath(video_list_xpath)
            try:
                for one_video in vidoe_list_obj:
                    if_pay = ""
                    str_res_list = one_video.text.split("\n")
                    if len(str_res_list) == 4:
                        play_count_str, if_pay, title_str, video_count_str = str_res_list
                    elif len(str_res_list) == 3:
                        play_count_str, title_str, video_count_str = str_res_list
                    else:
                        play_count_str, title_str = str_res_list
                        video_count_str = ""
                    play_count = trans_play_count(play_count_str)
                    project_name = "bilibili_%s" % title_str
                    url = one_video.find_element_by_xpath("./a[1]").get_attribute('href')
                    data_dic = {
                            "play_count": play_count,
                            "url": url,
                            "title": title_str,
                            "video_count": video_count_str,
                            "if_pay": if_pay
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
                    self.parse_data(data_dic, project_name)
                if next_page_obj:
                    # next_page_obj[0].click()
                    action = ActionChains(self.driver)
                    action.click(next_page_obj[0]).perform()
                else:
                    # self.driver.close()
                    break
            except Exception as e:
                print(e)
                self.driver.close()

    def style_tags(self, url=None,
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
        self.driver.get(url)
        time.sleep(0.2)
        style_tags_obj = self.driver.find_elements_by_xpath(style_tags_xpath)
        for style_tag in style_tags_obj:
            style_tags = style_tag.text
            print(style_tags)
            action = ActionChains(self.driver)
            action.click(style_tag).perform()
            if style_tags == "全部":
                continue
            year=style_tags
        # if True:
            self.list_page(url=self.driver.current_url,
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
                           roll=roll,)

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
            res.update(data_dic)
            rds_single.hmset(project_name, res)
        else:
            data = copy.deepcopy(self.single_video_dic)
            data.update(data_dic)
            rds_single.hmset(project_name, data)


    def video_page(self,task=0):
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
            self.driver.get(res["url"])
            self.driver.execute_script("window.scrollBy(0,1000)")
            self.driver.implicitly_wait(10)
            title = self.driver.find_element_by_xpath("//a[@class='media-title']").text
            print("task ",str(task),title)
            detail_page_url_obj = self.driver.find_element_by_xpath("//a[@class='media-title']")
            detail_page_url = detail_page_url_obj.get_attribute("href")
            print(detail_page_url)
            time.sleep(1)
            discribe_info = self.driver.find_element_by_xpath("//div[@class='media-count']")
            discribe_info_str = discribe_info.text
            play_count,barrage_count,favorite_count = discribe_info_str.split("  ·  ")
            # print(discribe_info_str)
            play_count=trans_play_count(play_count)
            if not play_count:
                print(discribe_info_str)
                play_count="--"
            rate_list = self.driver.find_elements_by_xpath("//div[@class='media-rating']")
            if rate_list:
                rate = rate_list[0].text
            else:
                rate = ""
            # print(rate)
            describe = self.driver.find_element_by_xpath("//a[@class='media-desc webkit-ellipsis']").text
            describe = describe.split("\n",-1)[0]
            # print(describe)
            dic = {"describe":describe,
                   "rate":rate,
                   "play_count":play_count,
                   "barrage_count":barrage_count,
                   "favorite_count":favorite_count,
                   }
            project_name = "bilibili_%s" % title
            self.parse_data(dic,project_name)
            self.one_video_page(title,res["url"])
            rds_get.delete(keys)


    def one_video_page(self,title,url):
        video_obj_list = self.driver.find_elements_by_xpath("//div[@id='eplist_module']//li")
        if video_obj_list:
            # time.sleep(0.1)
            action = ActionChains(self.driver)
            video_name_tags = self.driver.find_elements_by_xpath("//i[@class='mode-change iconfont icon-ep-list-simple']")
            if video_name_tags:
            # time.sleep(0.1)
                action.move_to_element(video_name_tags[0]).click().perform()
                del action
            time.sleep(0.1)
            video_obj_list = self.driver.find_elements_by_xpath("//div[@id='eplist_module']//li")
            video_obj= self.driver.find_element_by_xpath("//div[@id='eplist_module']//li")
            for video_count,video_obj in enumerate(video_obj_list):
                self.driver.implicitly_wait(10)
                action = ActionChains(self.driver)
                action.click(video_obj).perform()
                del action
                self.driver.execute_script("window.scrollBy(0,1000)")
                time.sleep(0.2)
                video_title = video_obj.text
                if_pay = ""
                # print(video_title)
                if "\n" in video_title:
                    video_title,if_pay = video_title.split("\n",-1)
                # print(self.driver.page_source)
                comment_count_list = self.driver.find_elements_by_xpath("//span[@class='results']")
                if comment_count_list:
                    comment_count = comment_count_list[0].text
                else:
                    comment_count = 0
                # print(comment_count)
                video_id = self.driver.find_element_by_xpath("//a[@class='av-link']")
                video_url = video_id.get_attribute("href")
                # print(video_url)
                # print(video_id.text)
                barrage_count_list = self.driver.find_elements_by_xpath("//span[@class='bilibili-player-video-info-danmaku-number']")
                if barrage_count_list:
                    barrage_count = barrage_count_list[0].text
                else:
                    barrage_count = "-"
                # print(barrage_count)
                duration = self.driver.find_elements_by_xpath('//*[@id="bilibiliPlayer"]/div[1]/div[1]/div[10]/div[2]/div[2]/div[1]/div[3]/div/span[3]')
                try:
                    duration = trans_duration(duration[0].text)
                    print(video_count,duration)
                except:
                    duration = ""
                # print(duration)
                project_name = "bilibili_%s_%s" % (title,video_title)
                dic = {
                        "title": title,
                        "video_title": video_title,
                        "if_pay":if_pay,
                        "comment_count":comment_count,
                        "url":url,
                        "video_url":video_url,
                        "video_id":video_id.text,
                        "barrage_count":barrage_count,
                        "duration":duration,
                        "video_count":video_count + 1
                }
                self.parse_single_data(dic,project_name)
        else:
            self.driver.execute_script("window.scrollBy(0,1000)")
            # time.sleep(0.4)
            video_title = self.driver.find_element_by_xpath('//*[@id="bilibiliPlayer"]/div[1]/div[1]/div[1]/div[1]').text
            if_pay = ""
            # print(video_title)
            if "\n" in video_title:
                video_title, if_pay = video_title.split("\n", -1)
            # print(self.driver.page_source)
            comment_count = self.driver.find_element_by_xpath("//span[@class='results']").text
            # print(comment_count)
            video_id = self.driver.find_element_by_xpath("//a[@class='av-link']")
            video_url = video_id.get_attribute("href")
            # print(video_url)
            # print(video_id.text)
            barrage_count = self.driver.find_element_by_xpath(
                "//span[@class='bilibili-player-video-info-danmaku-number']").text
            # print(barrage_count)
            duration = self.driver.find_elements_by_xpath(
                '//*[@id="bilibiliPlayer"]/div[1]/div[1]/div[10]/div[2]/div[2]/div[1]/div[3]/div/span[3]')
            try:
                duration = trans_duration(duration[0].text)
                print(duration)
            except:
                duration = 0
            project_name = "bilibili_%s_%s" % (title, video_title)
            dic = {
                    "title": title,
                    "video_title": video_title,
                    "if_pay": if_pay,
                    "comment_count": comment_count,
                    "url": url,
                    "video_url": video_url,
                    "video_id": video_id.text,
                    "barrage_count": barrage_count,
                    "duration": duration,
                    "video_count":1
            }
            self.parse_single_data(dic, project_name)


    def detail_page(self):
        pass

if __name__ == "__main__":
    # crlawler_bilbili = CrawlerMain()
    # crlawler_bilbili.list_page(url="https://www.bilibili.com/documentary/index/#style_id=-1&producer_id=-1&release_date=-1&season_status=1&order=2&st=3&sort=0&page=1",
    #                            video_list_xpath="//ul[@class='bangumi-list clearfix']//li",
    #                            next_page_xpath="//a[@class='p next-page']",
    #                            roll=1200
    #                            )
    # style_tags_dic = {
    #         "历史":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=10033&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    #         "美食":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=10045&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    #         "人文":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=10065&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    #         "科技":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=10066&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    #         "探险":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=10067&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    #         "宇宙":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=10068&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    #         "萌宠":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=10069&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    #         "社会":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=10070&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    #         "动物":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=10071&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    #         "自然":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=10072&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    #         "医疗":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=10073&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    #         "军事":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=10074&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    #         "灾难":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=10064&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    #         "罪案":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=10075&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    #         "神秘":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=10076&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    #         "旅行":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=10077&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    #         "运动":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=10038&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    #         "电影":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=-10&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
    # }
    # for style in style_tags_dic:
    #     try:
    #         crlawler_bilbili = CrawlerMain()
    #         crlawler_bilbili.style_tags(
    #             url=style_tags_dic[style],
    #             video_list_xpath="//ul[@class='bangumi-list clearfix']//li",
    #             next_page_xpath="//a[@class='p next-page']",
    #             roll=1200,style=style
    #             )
    #     except:
    #         continue

#     project_dic = {
#         "央视":"https://www.bilibili.com/documentary/index/#style_id=-1&producer_id=4&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
#         "BBC":"https://www.bilibili.com/documentary/index/#style_id=-1&producer_id=1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
#         "探索频道":"https://www.bilibili.com/documentary/index/#style_id=-1&producer_id=7&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
#         "NHK":"https://www.bilibili.com/documentary/index/#style_id=-1&producer_id=2&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
#         "历史频道":"https://www.bilibili.com/documentary/index/#style_id=-1&producer_id=6&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
#         "卫视":"https://www.bilibili.com/documentary/index/#style_id=-1&producer_id=8&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
#         "自制":"https://www.bilibili.com/documentary/index/#style_id=-1&producer_id=9&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
#         "ITV":"https://www.bilibili.com/documentary/index/#style_id=-1&producer_id=5&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
#         "SKY":"https://www.bilibili.com/documentary/index/#style_id=-1&producer_id=3&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
#         "ZDF":"https://www.bilibili.com/documentary/index/#style_id=-1&producer_id=10&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
#         "合作机构":"https://www.bilibili.com/documentary/index/#style_id=-1&producer_id=11&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
#         "国内其他":"https://www.bilibili.com/documentary/index/#style_id=-1&producer_id=12&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
#         "国外其他":"https://www.bilibili.com/documentary/index/#style_id=-1&producer_id=13&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
#
# }
#     for project in project_dic:
#         try:
#             crlawler_bilbili = CrawlerMain()
#             crlawler_bilbili.style_tags(
#                 url=project_dic[project],
#                 video_list_xpath="//ul[@class='bangumi-list clearfix']//li",
#                 next_page_xpath="//a[@class='p next-page']",
#                 roll=1200,project_tag=project
#                 )
#         except:
#             continue
#     project_dic = {
#         "2020":"https://www.bilibili.com/documentary/index/?spm_id_from=333.6.b_7375626e6176.6#style_id=-1&producer_id=-1&release_date=-1&season_status=-1&order=2&st=3&sort=0&page=1",
# }
#     crlawler_bilbili = CrawlerMain()
#     for project in project_dic:
#         crlawler_bilbili.style_tags(
#             url=project_dic[project],
#             video_list_xpath="//ul[@class='bangumi-list clearfix']//li",
#             next_page_xpath="//a[@class='p next-page']",
#             roll=1200,year=project,
#         style_tags_xpath="/html[1]/body[1]/div[2]/div[2]/div[2]/div[2]/div[3]/ul[1]/li"
#             )




    executor = ProcessPoolExecutor(max_workers=6)
    futures = []
    crlawler_bilbili = CrawlerMain()
    # process_task = crlawler_bilbili.video_page
    for one_scan in range(3):
        crlawler_bilbili = CrawlerMain()
        future = executor.submit(crlawler_bilbili.video_page,task=one_scan)
        futures.append(future)

        # crlawler_bilbili.video_page(one_scan)
    executor.shutdown(True)
    # crlawler_bilbili = CrawlerMain()
    # crlawler_bilbili.video_page()

