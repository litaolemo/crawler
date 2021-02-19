# -*- coding: utf-8 -*-
"""
Created on Tue Nov 27 14:05:18 2018

@author: fangyucheng
"""

import hashlib
import uuid
import copy
import urllib
import time
import datetime
import requests
from crawler.crawler_sys.framework.video_fields_std import Std_fields_video
from crawler.crawler_sys.utils.trans_duration_str_to_second import trans_duration
from crawler.crawler_sys.site_crawler import crawler_v_qq
from crawler.crawler_sys.utils.output_results import output_result
import re,json

try:
    from crawler_sys.framework.func_get_releaser_id import *
except:
    from func_get_releaser_id import *
from crawler.crawler_sys.proxy_pool.func_get_proxy_form_kuaidaili import get_proxy


class Crawler_Tencent_News():

    def __init__(self, platform='腾讯新闻'):
        self.platform = platform
        self.devid = '008796749793280'
        # self.appver = '23_android_5.4.10'
        # self.devid = "7313ae71df9e5367",
        self.appver = "23_android_5.8.00"
        self.qnrid = str(uuid.uuid4())
        self.headers = {"Host": "r.inews.qq.com",
                        "Accept-Encoding": "gzip,deflate",
                        "Referer": "http://inews.qq.com/inews/android/",
                        "User-Agent": "%E8%85%BE%E8%AE%AF%E6%96%B0%E9%97%BB5410(android)",
                        "Cookie": "lskey=;luin=;skey=;uin=; logintype=0; main_login=qq;",
                        "Connection": "Keep-Alive"}
        self.video_data = Std_fields_video().video_data
        self.video_data['platform'] = self.platform
        untouch_key_lst = ['channel', 'describe', 'isOriginal', 'repost_count']
        for key in untouch_key_lst:
            self.video_data.pop(key)
        self.crawler_video_page = crawler_v_qq.Crawler_v_qq().video_page
        self.list_page_dict = {"体育": "8"}


    def get_releaser_id(self, releaserUrl):
        return get_releaser_id(platform=self.platform, releaserUrl=releaserUrl)

    def forllower_num_to_int(self, num):
        if str(num)[-1:] == "万":
            return int(float(num[:-1]) * 1e4)
        elif isinstance(num, float):
            return num
        elif isinstance(num, int):
            return num

    def get_releaser_follower_num(self, releaserUrl):

        head = {
                "chlid": self.get_releaser_id(releaserUrl),
                "media_openid": "",
                "is_special_device": "0",
                "mid": "0",
                "dpi": "270.0",
                "is_chinamobile_oem": "0",
                "qqnetwork": "wifi",
                "rom_type": "V417IR release-keys",
                "real_device_width": "3.0",
                "net_proxy": "DIRECT@",
                "net_bssid": "01:80:c2:00:00:03",
                "currentChannelId": "news_video_child_newRecommend",
                "isElderMode": "0",
                "apptype": "android",
                "islite": "0",
                "hw": "Xiaomi_MINOTE3",
                "global_session_id": "1558345020297",
                "screen_width": "810",
                "omgbizid": "",
                "sceneid": "",
                "videoAutoPlay": "1",
                "imsi": "460063005313888",
                "fix_store": "",
                "isoem": "0",
                "currentTabId": "news_live",
                "lite_version": "",
                "net_slot": "0",
                "startTimestamp": "1558345020",
                "pagestartfrom": "icon",
                "mac": "mac unknown",
                "activefrom": "icon",
                "net_ssid": "NemuWiFi",
                "store": "10611",
                "screen_height": "1440",
                "extinfo": "",
                "real_device_height": "5.33",
                "origin_imei": "261721032526201",
                "network_type": "wifi",
                "origCurrentTab": "live",
                "global_info": "1|1|1|1|1|14|4|1|0|6|1|1|1||0|J309P000000000:J601P000000000:A401P000050901:J401P100000000:J304P000000000:J701P000000000:B703P000062002:B704P000064803:J702P000000000:B064P000065702:J267P000000000:B060P000066504:A403P000070903:J055P000000000:A402P000060701:B402P200065202:B054P100068903:A054P200070501:A054P600071201:A054P300068801:J054P000000000:J054P040000000|1414|0|1|0|0|0|0|0||3|3|1|1|1|1|1|1|-1|0|0|2|2|0|0|0|4|0|0|1|0|5|2|0|0|3|0|0|1|0|1|1|0|0|1|0|4|0|1|2|11|20|1|0|1|0|0|30|1|4|0|0|3|40|0|51|60|0|0|0|0|0",
                "imsi_history": "460063005313888",
                "net_apn": "0",
                "uid": "7313ae71df9e5367",
                "omgid": "",
                "trueVersion": "5.8.00",
                "qimei": "261721032526201",
                "devid": "7313ae71df9e5367",
                "appver": "23_android_5.8.00",
                "Cookie": "lskey=;skey=;uin=; luin=;logintype=0; main_login=",

        }
        try:
            url = "https://r.inews.qq.com/getSubItem?%s" % urllib.parse.urlencode(head)
            res = requests.get(url)
            res_json = res.json()
            # print(res_json)
            follower_num = self.forllower_num_to_int(res_json.get("channelInfo").get("subCount"))
            releaser_img = self.get_releaser_image(data=res_json)
            print('%s follower number is %s' % (releaserUrl, follower_num))
            return follower_num,releaser_img
        except:
            print("can't find followers")

    def get_releaser_image(self, releaserUrl=None,data=None):
        if releaserUrl:
            head = {
                    "chlid": self.get_releaser_id(releaserUrl),
                    "media_openid": "",
                    "is_special_device": "0",
                    "mid": "0",
                    "dpi": "270.0",
                    "is_chinamobile_oem": "0",
                    "qqnetwork": "wifi",
                    "rom_type": "V417IR release-keys",
                    "real_device_width": "3.0",
                    "net_proxy": "DIRECT@",
                    "net_bssid": "01:80:c2:00:00:03",
                    "currentChannelId": "news_video_child_newRecommend",
                    "isElderMode": "0",
                    "apptype": "android",
                    "islite": "0",
                    "hw": "Xiaomi_MINOTE3",
                    "global_session_id": "1558345020297",
                    "screen_width": "810",
                    "omgbizid": "",
                    "sceneid": "",
                    "videoAutoPlay": "1",
                    "imsi": "460063005313888",
                    "fix_store": "",
                    "isoem": "0",
                    "currentTabId": "news_live",
                    "lite_version": "",
                    "net_slot": "0",
                    "startTimestamp": "1558345020",
                    "pagestartfrom": "icon",
                    "mac": "mac unknown",
                    "activefrom": "icon",
                    "net_ssid": "NemuWiFi",
                    "store": "10611",
                    "screen_height": "1440",
                    "extinfo": "",
                    "real_device_height": "5.33",
                    "origin_imei": "261721032526201",
                    "network_type": "wifi",
                    "origCurrentTab": "live",
                    "global_info": "1|1|1|1|1|14|4|1|0|6|1|1|1||0|J309P000000000:J601P000000000:A401P000050901:J401P100000000:J304P000000000:J701P000000000:B703P000062002:B704P000064803:J702P000000000:B064P000065702:J267P000000000:B060P000066504:A403P000070903:J055P000000000:A402P000060701:B402P200065202:B054P100068903:A054P200070501:A054P600071201:A054P300068801:J054P000000000:J054P040000000|1414|0|1|0|0|0|0|0||3|3|1|1|1|1|1|1|-1|0|0|2|2|0|0|0|4|0|0|1|0|5|2|0|0|3|0|0|1|0|1|1|0|0|1|0|4|0|1|2|11|20|1|0|1|0|0|30|1|4|0|0|3|40|0|51|60|0|0|0|0|0",
                    "imsi_history": "460063005313888",
                    "net_apn": "0",
                    "uid": "7313ae71df9e5367",
                    "omgid": "",
                    "trueVersion": "5.8.00",
                    "qimei": "261721032526201",
                    "devid": "7313ae71df9e5367",
                    "appver": "23_android_5.8.00",
                    "Cookie": "lskey=;skey=;uin=; luin=;logintype=0; main_login=",

            }
            try:
                url = "https://r.inews.qq.com/getSubItem?%s" % urllib.parse.urlencode(head)
                res = requests.get(url)
                res_json = res.json()
                # print(res_json)
                releaser_img_url = res_json.get("channelInfo").get("icon")
                return releaser_img_url
            except:
                print("can't get releaser_img_url")
        else:
            releaser_img_url = data.get("channelInfo").get("icon")
            return releaser_img_url


    # def search_video_page(self, keyword, releaser_url,releaser=True,
    #                       search_pages_max=30,
    #                       output_to_es_raw=False,
    #                       output_to_es_register=False,
    #                       es_index=None,
    #                       doc_type=None,
    #                       **kwargs):
    #     """
    #     This is improved search page crawler, involved search_type.
    #     When search_type == 'searchMore', it's the same as previous one,
    #     which is the '综合' column in app search page.
    #     When search_type == 'verticalSearch', it's is the '视频' column
    #     in app search page.
    #     """
    #
    #     def get_list(search_type, page_dict):
    #         # print(search_type,page_dict)
    #         if search_type == 'searchMore':
    #             return page_dict['data']['secData']
    #         elif search_type == 'verticalSearch':
    #             try:
    #                 return page_dict['secList'][0].get('videoList')
    #             except:
    #                 return []
    #         else:
    #             print('unknow search_type:', search_type)
    #             return None
    #
    #     search_request_dict = {
    #             'verticalSearch': {
    #                     'url_prefix': 'http://r.inews.qq.com/verticalSearch?',
    #                     'para_dict': {
    #                             "chlid": "_qqnews_custom_search_video",
    #                             "uid": "7313ae71df9e5367",
    #                             "omgid": "",
    #                             "trueVersion": "5.8.00",
    #                             "qimei": "379317519303213",
    #                             "devid": "008796749793280",
    #                             "appver": "23_android_5.8.00",
    #                             "Cookie": "lskey=;skey=;uin=; luin=;logintype=0; main_login=;",
    #                     },
    #             }
    #     }
    #
    #     headers = {"Host": "r.inews.qq.com",
    #                "Accept-Encoding": "gzip",
    #                "Referer": "http://inews.qq.com/inews/android/",
    #                "Content-Type": "application/x-www-form-urlencoded",
    #                "User-Agent": "%E8%85%BE%E8%AE%AF%E6%96%B0%E9%97%BB5800(android)",
    #                "RecentUserOperation": "2_GuidePage,1_news_background,1_news_news_top",
    #                "Connection": "Keep-Alive"}
    #
    #     body = {
    #             "search_type": "video",
    #             "query": keyword,
    #             "page": "1",
    #             "type": "0",
    #             "transparam": '{"sessionid":"2015601560736100"}',
    #             "search_from": "click",
    #             "cp_type": "0",
    #             "is_special_device": "0",
    #             "mid": "0",
    #             "dpi": "270.0",
    #             "is_chinamobile_oem": "0",
    #             "qqnetwork": "wifi",
    #             "rom_type": "V417IR release-keys",
    #             "real_device_width": "3.0",
    #             "net_proxy": "DIRECT@",
    #             "net_bssid": "01:80:c2:00:00:03",
    #             "currentChannelId": "news_news_top",
    #             "isElderMode": "0",
    #             "apptype": "android",
    #             "islite": "0",
    #             "hw": "HUAWEI_BLA-AL00",
    #             "global_session_id": "1560735392163",
    #             "screen_width": "810",
    #             "videoAutoPlay": "1",
    #             "imsi": "460062614015394",
    #             "isoem": "0",
    #             "currentTabId": "news_news",
    #             "net_slot": "0",
    #             "startTimestamp": "0",
    #             "pagestartfrom": "icon",
    #             "mac": "mac unknown",
    #             "activefrom": "icon",
    #             "net_ssid": "WiFi",
    #             "store": "10611",
    #             "screen_height": "1440",
    #             "real_device_height": "5.33",
    #             "origin_imei": "379317519303213",
    #             "network_type": "wifi",
    #             "origCurrentTab": "top",
    #             "global_info": "1|1|1|1|1|14|4|1|0|6|1|1|1||0|J309P000000000:A601P000081702:A401P000050901:J401P100000000:J602P000000000:J304P000000000:B701P000075404:J703P000000000:B704P000085403:J702P000000000:B064P000065702:A267P000074401:B267P100078102:B060P000085902:J403P000000000:J403P100000000:J055P200000000:A402P100080401:J402P000000000:J402P200000000:B054P000061502:A054P600071201:J054P200000000:J054P100000000|1414|0|1|0|0|0|0|0||3|3|1|1|1|1|1|1|-1|0|0|2|2|0|0|0|4|0|0|1|2|5|2|0|0|3|0|0|1|0|1|1|0|0|1|0|4|0|1|2|11|20|1|0|1|0|0|30|1|4|0|1|4|40|0|51|60|0|0|0|0|0|4|0|0|0|0",
    #             "imsi_history": "460062614015394",
    #             "net_apn": "0"
    #     }
    #     final_lst = []
    #     for search_type in search_request_dict:
    #         print(datetime.datetime.now(), '****** search_type', search_type)
    #         qnrid, qnsig = self.build_qnrid_and_qnsig(cgi=search_type)
    #         for page in range(1, search_pages_max):
    #             para_dict = search_request_dict[search_type]['para_dict']
    #             body['page'] = page
    #             url_prefix = search_request_dict[search_type]['url_prefix']
    #             url = url_prefix + urllib.parse.urlencode(para_dict)
    #
    #             get_page = requests.post(url, headers=headers, data=body)
    #             try:
    #                 page_dict = get_page.json()
    #             except:
    #                 pass
    #             else:
    #                 video_lst = get_list(search_type, page_dict)
    #                 if video_lst is not None and video_lst != []:
    #                     for video_dict in video_lst:
    #                         if 'hasVideo' in video_dict:
    #                             try:
    #                                 if 'source' not in video_dict:
    #                                     # ignore those without 'source' field
    #                                     continue
    #                                 video_info = copy.deepcopy(self.video_data)
    #                                 info_id = video_dict['id']
    #                                 video_info['title'] = video_dict['title']
    #                                 video_info['url'] = video_dict['url']
    #                                 video_info['video_id'] = video_dict['vid']
    #                                 video_info['play_count'] = video_dict.get('video_channel').get('video').get(
    #                                         'playcount')
    #                                 video_info['releaser'] = video_dict['source']
    #                                 try:
    #                                     video_info['releaserUrl'] = releaser_url
    #                                     video_info['releaser_id_str'] = "腾讯新闻_" + self.get_releaser_id(releaser_url)
    #                                 except:
    #                                     video_info['releaserUrl'] = ""
    #                                     video_info['releaser_id_str'] = ""
    #                                 video_info['release_time'] = int(video_dict['timestamp'] * 1e3)
    #                                 video_info['favorite_count'] = video_dict['likeInfo']
    #                                 video_info['comment_count'] = video_dict['comments']
    #                                 try:
    #                                     dura_str = trans_duration(
    #                                             video_dict.get('video_channel').get('video').get('duration'))
    #                                 except:
    #                                     dura_str = ''
    #                                 video_info['duration'] = dura_str
    #                                 fetch_time = int(datetime.datetime.now().timestamp() * 1e3)
    #                                 video_info['fetch_time'] = fetch_time
    #                                 if releaser:
    #                                     if keyword == video_info['releaser']:
    #                                         final_lst.append(video_info)
    #                                 else:
    #                                     final_lst.append(video_info)
    #                                 # print(video_info)
    #                                 print("get video data %s" % video_info['title'])
    #                             except:
    #                                 continue
    #                         else:
    #                             print("hasVideo flag is False, no data in this video_dict")
    #                             break
    #                         if len(final_lst) >= 100:
    #                             output_result(result_Lst=final_lst,
    #                                           platform=self.platform,
    #                                           output_to_es_raw=output_to_es_raw,
    #                                           output_to_es_register=output_to_es_register,
    #                                           es_index=es_index,
    #                                           doc_type=doc_type)
    #                             final_lst.clear()
    #
    #     if final_lst != []:
    #         output_result(result_Lst=final_lst,
    #                       platform=self.platform,
    #                       output_to_es_raw=output_to_es_raw,
    #                       output_to_es_register=output_to_es_register,
    #                       es_index=es_index,
    #                       doc_type=doc_type)
    #     return final_lst

    def releaser_page(self, releaserUrl,
                      output_to_file=False, filepath=None,
                      output_to_es_raw=False,
                      output_to_es_register=False,
                      push_to_redis=False,
                      releaser_page_num_max=10000,
                      es_index=None,
                      doc_type=None,
                      proxies_num=None):
        proxies = get_proxy(proxies_num)
        releaser_id = self.get_releaser_id(releaserUrl)
        # qnrid, qnsig = self.build_qnrid_and_qnsig(cgi="om")
        result_list = []
        has_more = True
        count = 1
        page_time = ""
        while has_more and count <= releaser_page_num_max and releaser_id:
            url_dic = {
                    "chlid": releaser_id,
                    "page_time": page_time,
                    "coral_uin": "ec8bb1459b9d84100312bf035bb43cd4d0",
                    "coral_uid": "",
                    "type": "om",
                    "uid": "7313ae71df9e5367",
                    "omgid": "",
                    "trueVersion": "5.8.00",
                    "qimei": "287801615436009",
                    # "devid"	:"7313ae71df9e5367",
                    # "appver"	:"23_android_5.8.00",
                    'devid': self.devid,
                    'appver': self.appver,

            }
            post_body = {

                    "activefrom": "icon",
                    "apptype": "android",
                    "article_pos": "0",
                    "articleID": "ec8bb1459b9d84100312bf035bb43cd4d0_%s" % releaser_id,
                    "articlepage": "-1",
                    "articletype": "509",
                    "articleType": "509",
                    "articleUUID": "7a7f71aff201a175cbb8b946b1a0ab3b",
                    "cell_id": "normal_article_cell",
                    "cityList": "news_news_bj",
                    "coverType": "0",
                    "currentChannelId": "news_news_top",
                    "currentTabId": "news_news",
                    "dpi": "270.0",
                    "global_info": "1|1|1|1|1|14|4|1|0|6|1|1|1||0|J309P000000000:J902P000000000:J601P000000000:A601P400109701:A601P200096101:J601P500000000:J601P100000000:J601P600000000:J601P300000000:J603P000000000:J604P000000000:A401P000050901:J401P100000000:J602P000000000:J602P900000000:J304P000000000:J701P000000000:B703P000107302:J704P000000000:B702P000098602:A064P000117303:B085P000087702:B267P000118602:J267P100000000:B073P000120202:A060P000116701:J060P400000000:J060P100000000:J060P016000000:A403P100114101:J403P000000000:J055P000000000:J055P200000000:B402P100095203:J402P000000000:J402P013000000:A054P000101101:A054P600071201:J054P200000000:B901P000117402|1414|0|1|24|24|0|0|0||3|3|1|1|1|1|1|1|-1|0|0|2|2|0|0|0|4|0|0|1|2|5|2|0|0|3|0|0|1|0|1|1|0|0|1|0|4|0|1|1|11|20|1|0|1|1|0|0|1|4|0|1|1|40|0|51|60|0|0|0|0|0|4|0|0|0|0|0|0",
                    "global_session_id": "1564032931171",
                    "hasVideo": "0",
                    "hw": "vivo_VIVOX20Plus",
                    "id": "ec8bb1459b9d84100312bf035bb43cd4d0_%s" % releaser_id,
                    "idStr": "ec8bb1459b9d84100312bf035bb43cd4d0_%s" % releaser_id,
                    "imsi": "460073046925329",
                    "imsi_history": "460073046925329",
                    "is_chinamobile_oem": "0",
                    "is_special_device": "0",
                    "isAd": "0",
                    "isCpFocus": "0",
                    "isElderMode": "0",
                    "isGifPlayed": "0",
                    "isHotCommentLink": "0",
                    "isHotNews": "0",
                    "isIPSpecialVideo": "0",
                    "islite": "0",
                    "isoem": "0",
                    "mac": "mac unknown",
                    "mid": "0",
                    "moduleArticlePos": "0",
                    "net_apn": "0",
                    "net_bssid": "49:4a:55:76:75:58",
                    "net_proxy": "DIRECT@",
                    "net_slot": "0",
                    "net_ssid": "IJUvuXkoA8H",
                    "network_type": "wifi",
                    "newsID": "ec8bb1459b9d84100312bf035bb43cd4d0_%s" % releaser_id,
                    "origCurrentTab": "top",
                    "origin_imei": "287801615436009",
                    "originPageType": "second_timeline",
                    "page_type": "second_timeline",
                    "pageIsIPSpecialVideo": "0",
                    "pagestartfrom": "icon",
                    "qqnetwork": "wifi",
                    "real_device_height": "5.33",
                    "real_device_width": "3.0",
                    "realArticlePos": "0",
                    "rom_type": "V417IR release-keys",
                    "screen_height": "1440",
                    "screen_width": "810",
                    "startTimestamp": "0",
                    "store": "10611",
                    #"title": "看看新闻Knews",
                    "userId": "ec8bb1459b9d84100312bf035bb43cd4d0",
                    "userMediaId":  releaser_id,
                    "userVipType": "0",
                    "videoAutoPlay": "1",
                    "videoBlackBorder": "0",
                    "videoShowType": "0",

            }
            post_url = "https://r.inews.qq.com/getUserVideoList?"
            url = post_url + urllib.parse.urlencode(url_dic)
            if proxies:
                get_page = requests.post(url, headers=self.headers, data=post_body,proxies=proxies)
            else:
                get_page = requests.post(url, headers=self.headers, data=post_body)
            page_dic = {}
            try:
                page_dic = get_page.json()
                # print(page_dic)
                data_list = page_dic.get('newslist')
                has_more = page_dic.get('next')
                page_time = str(page_dic.get("last_time"))
            except:
                if data_list:
                    data_list = page_dic.get('newslist')
                    has_more = page_dic.get('next')
                else:
                    data_list = []
                    has_more = False
            # offset = page_dic.get('offset')

            if has_more is None:
                has_more = False
            if data_list == []:
                proxies = get_proxy(1)
                print("no data in releaser %s page %s" % (releaser_id, count))
                # print(page_dic)
                # print(url)
                count += 1
                has_more = False
                continue
            else:
                count += 1
                print("craw data in releaser %s page %s" % (releaser_id, count))
                for one_video in data_list:
                    # info_str = one_video.get('content')
                    video_dic = copy.deepcopy(self.video_data)
                    video_dic['title'] = one_video.get('title')
                    video_dic['url'] = one_video.get('url')
                    video_dic['releaser'] = one_video.get('chlname')
                    video_dic['releaserUrl'] = "https://view.inews.qq.com/media/%s" % releaser_id
                    release_time = one_video.get('timestamp')
                    video_dic['release_time'] = int(release_time * 1e3)
                    video_dic['duration'] = int(self.t2s(one_video.get('videoTotalTime')))
                    if not video_dic['duration']:
                        try:
                            video_dic['duration'] = int(self.t2s(one_video.get('video_channel').get("video").get("duration")))
                        except:
                            video_dic['duration'] = 0
                    video_dic['play_count'] = one_video.get('video_channel').get("video").get("playcount")
                    video_dic['repost_count'] = one_video.get('shareCount')
                    video_dic['comment_count'] = one_video.get('comments')
                    video_dic['favorite_count'] = one_video.get('likeInfo')
                    video_dic['video_id'] = one_video.get('vid')
                    video_dic['fetch_time'] = int(datetime.datetime.now().timestamp() * 1e3)
                    video_dic['releaser_id_str'] = "腾讯新闻_%s" % releaser_id
                    video_dic['video_img'] = one_video.get('pic_minivideo')
                    yield video_dic

    @staticmethod
    def t2s(t):
        if t:
            if len(t) == 5:
                t = str(t)
                m, s = t.strip().split(":")
                return float(m) * 60 + float(s)
            elif len(t) >= 7:
                t = str(t)
                h, m, s = t.strip().split(":")
                return float(h) * 3600 + float(m) * 60 + float(s)
        else:
            return 0

    # def search_page(self, keyword,
    #                 search_pages_max=30,
    #                 output_to_es_raw=False,
    #                 output_to_es_register=False,
    #                 es_index=None,
    #                 doc_type=None):
    #     """
    #     This is improved search page crawler, involved search_type.
    #     When search_type == 'searchMore', it's the same as previous one,
    #     which is the '综合' column in app search page.
    #     When search_type == 'verticalSearch', it's is the '视频' column
    #     in app search page.
    #     """
    #
    #     def get_list(search_type, page_dict):
    #         # print(search_type,page_dict)
    #         if search_type == 'searchMore':
    #             return page_dict['data']['secData']
    #         elif search_type == 'verticalSearch':
    #             try:
    #                 return page_dict['secList'][0].get('videoList')
    #             except:
    #                 return []
    #         else:
    #             print('unknow search_type:', search_type)
    #             return None
    #
    #     search_request_dict = {
    #             'verticalSearch': {
    #                     'url_prefix': 'http://r.inews.qq.com/verticalSearch?',
    #                     'para_dict': {
    #                             'devid': self.devid,
    #                             'appver': self.appver,
    #                             'query': keyword,
    #                             'page': None,
    #                             'search_type': 'video'
    #                     },
    #             },
    #             'searchMore': {
    #                     'url_prefix': 'http://r.inews.qq.com/searchMore?',
    #                     'para_dict': {
    #                             'devid': self.devid,
    #                             'appver': self.appver,
    #                             'query': keyword,
    #
    #                             'page': None,
    #                     }
    #             },
    #     }
    #
    #     final_lst = []
    #     for search_type in search_request_dict:
    #         print(datetime.datetime.now(), '****** search_type', search_type)
    #         for page in range(1, search_pages_max):
    #             para_dict = search_request_dict[search_type]['para_dict']
    #             para_dict['page'] = page
    #             url_prefix = search_request_dict[search_type]['url_prefix']
    #             url = url_prefix + urllib.parse.urlencode(para_dict)
    #
    #             get_page = requests.get(url, headers=self.headers)
    #             try:
    #                 page_dict = get_page.json()
    #             except:
    #                 pass
    #             else:
    #                 video_lst = get_list(search_type, page_dict)
    #                 if video_lst is not None and video_lst != []:
    #                     for video_dict in video_lst:
    #                         if 'hasVideo' in video_dict:
    #                             try:
    #                                 if 'source' not in video_dict:
    #                                     # ignore those without 'source' field
    #                                     continue
    #                                 video_info = copy.deepcopy(self.video_data)
    #                                 info_id = video_dict['id']
    #                                 playcnt_url = ('http://r.inews.qq.com/getSimpleNews'
    #                                                '/23_android_5.4.10/news_news_search/%s'
    #                                                % info_id)
    #                                 play_count, vid, info_source, data_source = self.get_playcnt(url=playcnt_url)
    #                                 video_info['title'] = video_dict['title']
    #                                 video_info['url'] = video_dict['url']
    #                                 video_info['video_id'] = vid
    #                                 video_info['play_count'] = play_count
    #                                 video_info['playcnt_url'] = playcnt_url
    #                                 video_info['releaser'] = video_dict['source']
    #                                 video_info['release_time'] = int(video_dict['timestamp'] * 1e3)
    #                                 video_info['info_source'] = info_source
    #                                 video_info['data_source'] = data_source
    #
    #                                 try:
    #                                     dura_str = video_dict['videoTotalTime']
    #                                 except:
    #                                     dura_str = ''
    #                                 video_info['duration'] = trans_duration(dura_str)
    #                                 fetch_time = int(time.time() * 1e3)
    #                                 video_info['fetch_time'] = fetch_time
    #                                 final_lst.append(video_info)
    #                                 print("get video data %s" % video_info['title'])
    #                             except:
    #                                 continue
    #                         else:
    #                             print("hasVideo flag is False, no data in this video_dict")
    #                         if len(final_lst) >= 100:
    #                             output_result(result_Lst=final_lst,
    #                                           platform=self.platform,
    #                                           output_to_es_raw=output_to_es_raw,
    #                                           output_to_es_register=output_to_es_register,
    #                                           es_index=es_index,
    #                                           doc_type=doc_type)
    #                             final_lst.clear()
    #
    #     if final_lst != []:
    #         output_result(result_Lst=final_lst,
    #                       platform=self.platform,
    #                       output_to_es_raw=output_to_es_raw,
    #                       output_to_es_register=output_to_es_register,
    #                       es_index=es_index,
    #                       doc_type=doc_type)
    #     return final_lst

    def get_playcnt(self, url):
        get_page = requests.get(url)
        page_dic = get_page.json()
        try:
            play_count = page_dic['attribute']['VIDEO_0']['playcount']
        except:
            play_count = None
        try:
            vid = page_dic['attribute']['VIDEO_0']['vid']
        except:
            vid = None
        if play_count is not None:
            info_source = 'tencent_news'
            data_source = None
        else:
            info_source = 'tencent_video'
            data_source = None
            if vid is not None:
                video_url = 'https://v.qq.com/x/page/%s.html' % vid
                added_dic = self.crawler_video_page(url=video_url)
                try:
                    play_count = added_dic['play_count']
                    data_source = added_dic['data_source']
                except:
                    play_count = None
                    data_source = None
        return play_count, vid, info_source, data_source

    def list_page_for_main_page(self,
                                channel_id,
                                output_to_file=False,
                                filepath=None,
                                list_page_max=30,
                                output_to_es_raw=False,
                                es_index=None,
                                doc_type=None,
                                output_to_es_register=False):

        post_dic = {'chlid': channel_id,
                    'forward': '2',
                    'uid': '6F0D5898-2C3A-46FE-9181-589BC52ED743',
                    'adReqData': '{"chid":2,"adtype":0,"pf":"iphone","launch":"0","ext":{"mob":{"mobstr":"Aejy45+NeSZw4VxymYnnIhMV+MEM+6sW9Rkw16FvkWGCz1rsPQflpTnsN+KnArzMwheqHiLErlbOlNWL0SoBI0lJtRh13iyR+LxSv3Y+hJrixm\\/Sxn\\/YhInAhlYioOjQ9cHGSSRmdgaDyqx2dDLZosKp+QSMqr649GGxQ36xbSdjbvZ3MGywBOsVNcf+EZkV+U9Q8LyDPc6PZ56b\\/GLGncf4XcrVFnKlUi+kebsg8DCD\\/nlvTDGSkWOtu33GJ4Ct\\/hfZ1c3UNHw5bRwHRM0L0+6QYANTrPzl2X6hZK3kijlJsub+RvcPNPNQGrhK3e4yYHJmspW19qE5mPgxd5lbwzJ8VQifTrjGeB+cdCcGmEPYBcZwHmxRhEAo7A0bJcSLK5KACWNsKw8I085yoKLCIE40\\/1J+umH8QsTU6K+wLdpjpaI6D3XMa\\/GZiguAcNB7HMSMpBFY6dq1saxz0u+6Ex2n2CwJlY4JYzf2S4r69t8J1WCQInAjIf\\/Io+ZVhXNnNUx3GVir\\/TaffnYpd\\/5ZvqdKtBIWXZFtXOoWC66tNBG\\/D+YAoY8\\/yVAQL7slsS1qbjdDqByVI2DMq299y6yAh0hejMouwaCGK2Q2OCMes5xrghJ1sotO5mSqioK23WbdF9GiQSVqmbE94wzpCwaPCwrEzkgKWHuPxh0UlqUs9QeGe30SHv4OOpqF9QOUeXYJ\\/Xkana90uC32g3LuM6jdPTv07qbyk1tX87pGdnyvjR9BBEhb0dyLUFi\\/Gx8t4T+yHLxt0X9yKsGKCJX1U8AdkTwLlJslIX9Rzqy+Yb1n9sg85KAS5yUsQZqSv9kKRuZpYsfj6LLaI\\/Bet9BUNtGu4hYuZBqKFWp34XegvS4d3M9U"}},"ver":"5.7.22","slot":[{"islocal":0,"orders_info":["67950414,6870997,0,1000,0,110,2","88685632,1266139,1761176905,19,101,110,3","48980066,1934913,3602870493,19,101,110,1"],"recent_rot":["1,2,3"],"refresh_type":0,"loid":"1,13,16,23","channel":"news_video_top"}],"appversion":"181210"}',
                    'kankaninfo': '{"gender":1,"lastExp":416,"refresh":0,"scene":2}',
                    'channelPosition': '1',
                    'rendType': 'kankan',
                    'page': '0'}

        headers = {"content-type": "application/x-www-form-urlencoded",
                   "store": "1",
                   "accept": "*/*",

                   "idft": "CE1E8744-7BF9-4FDD-87A5-463C6B9A66E1",
                   "idfa": "05571C2D-1C86-4B5B-87EF-E4B4DAF07DDB",
                   "appver": "12.0.1_qqnews_5.7.22",

                   "devid": "d605a70a-d084-487e-aaf1-8a057d40ef39",
                   "devicetoken": "<f4b49138 3ca95e38 1519836e daefaab6 799b04da c164f7a7 4cb7d999 6e343393>",
                   "accept-language": "zh-Hans-CN;q=1, en-CN;q=0.9",
                   "referer": "http://inews.qq.com/inews/iphone/",
                   "user-agent": "QQNews/5.7.22 (iPhone; iOS 12.0.1; Scale/2.00)",
                   "content-length": "2169",
                   "accept-encoding": "br, gzip, deflate",
                   "cookie": "logintype=2",
                   "qqnetwork": "wifi"}

        domain_url = "https://r.inews.qq.com/getQQNewsUnreadList?"

        query_dict = {'appver': '12.0.1_qqnews_5.7.22',
                      'pagestartfrom': 'icon',
                      'page_type': 'timeline',
                      'apptype': 'ios',
                      'rtAd': '1',
                      'imsi': '460-01',
                      'screen_height': '667',

                      'network_type': 'wifi',
                      'startTimestamp': '1545835451',
                      'store': '1',
                      'deviceToken': '<f4b49138 3ca95e38 1519836e daefaab6 799b04da c164f7a7 4cb7d999 6e343393>',
                      'global_info': '1|1|1|1|1|14|4|1|0|6|1|1|2|2|0|J267P000000000:J060P000000000:B054P000015802:J054P600000000|1421|0|1|0|0|0|0|0|1001|0|6|1|1|1|1|1|1|-1|0|0|0|2|1|1|0|0|2|0|1|0|4|0|0|0|3|0|0|0|0',
                      'globalInfo': '1|1|1|1|1|14|4|1|0|6|1|1|2|2|0|J267P000000000:J060P000000000:B054P000015802:J054P600000000|1421|0|1|0|0|0|0|0|1001|0|6|1|1|1|1|1|1|-1|0|0|0|2|1|1|0|0|2|0|1|0|4|0|0|0|3|0|0|0|0',
                      'screen_scale': '2',
                      'activefrom': 'icon',
                      'screen_width': '375',
                      'isJailbreak': '0',

                      'qqnews_refpage': 'QNCommonListController',
                      'omgid': 'a305486b92cc9e48f90929497de4cb30dfde0010112206',
                      'device_model': 'iPhone9,1',
                      'pagestartFrom': 'icon',
                      'device_appin': '6F0D5898-2C3A-46FE-9181-589BC52ED743',
                      'devid': 'D605A70A-D084-487E-AAF1-8A057D40EF39',
                      'omgbizid': '138dc6ef3ae8a24f7c897a9bbde8b9098f210060113210',
                      'idfa': '05571C2D-1C86-4B5B-87EF-E4B4DAF07DDB'}
        count = 0
        result_list = []
        while count < list_page_max:
            post_dic['page'] = str(count)
            timestamp = int(time.time())
            query_dict['startTimestamp'] = timestamp
            url = domain_url + urllib.parse.urlencode(query_dict)
            get_page = requests.post(url, data=post_dic, headers=headers)
            page_dict = get_page.json()
            # video_list1 = page_dict["kankaninfo"]["videos"]
            video_list2 = page_dict["newslist"]
            count += 1
            # return video_list2
            if video_list2 != []:
                print("get data at page %s" % str(count - 1))
            for video_info in video_list2:
                has_video = video_info.get('hasVideo')
                video_channel = video_info.get('video_channel')
                if has_video == 1 or video_channel is not None:
                    video_dict = copy.deepcopy(self.video_data)
                    video_dict['channel'] = channel_id
                    video_dict['title'] = video_info['longtitle']
                    print(video_dict['title'])
                    video_dict['url'] = video_info['url']
                    try:
                        dura_str = video_info['video_channel']['video']['duration']
                        video_dict['duration'] = trans_duration(dura_str)
                    except:
                        video_dict['duration'] = 0
                    try:
                        video_dict['releaser'] = video_info['chlname']
                    except:
                        video_dict['releaser'] = None
                    try:
                        video_dict['releaser_id'] = video_info['card']['uin']
                    except:
                        video_dict['releaser_id'] = None
                    video_dict['release_time'] = int(video_info['timestamp'] * 1e3)
                    try:
                        video_dict['read_count'] = video_info['read_count']
                    except:
                        video_dict['read_count'] = 0
                    video_dict['comment_count'] = video_info['comments']
                    video_dict['favorite_count'] = video_info['likeInfo']
                    try:
                        video_dict['play_count'] = video_info['video_channel']['video']['playcount']
                    except:
                        video_dict['play_count'] = 0
                    video_dict['article_id'] = video_info['id']
                    try:
                        video_dict['video_id_str'] = video_info['vid']
                    except:
                        video_dict['video_id_str'] = None
                    video_dict['fetch_time'] = int(time.time() * 1e3)
                    result_list.append(video_dict)
                    if len(result_list) >= 100:
                        output_result(result_Lst=result_list,
                                      platform=self.platform,
                                      output_to_file=output_to_file,
                                      filepath=filepath,
                                      output_to_es_raw=output_to_es_raw,
                                      es_index=es_index,
                                      doc_type=doc_type,
                                      output_to_es_register=output_to_es_register)
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
            return result_list

    def list_page_for_special_area(self,
                                   channel_id,
                                   output_to_file=False,
                                   filepath=None,
                                   list_page_max=30,
                                   output_to_es_raw=False,
                                   es_index=None,
                                   doc_type=None,
                                   output_to_es_register=False):
        page_list = []

        post_dic = {
                'adReqData': '{"chid":2,"adtype":0,"pf":"iphone","launch":"0","ext":{"mob":{"mobstr":"Aejy45+NeSZw4VxymYnnIhMV+MEM+6sW9Rkw16FvkWGCz1rsPQflpTnsN+KnArzMwheqHiLErlbOlNWL0SoBI0lJtRh13iyR+LxSv3Y+hJrixm\\/Sxn\\/YhInAhlYioOjQ9cHGSSRmdgaDyqx2dDLZosKp+QSMqr649GGxQ36xbSdjbvZ3MGywBOsVNcf+EZkV+U9Q8LyDPc6PZ56b\\/GLGncf4XcrVFnKlUi+kebsg8DCD\\/nlvTDGSkWOtu33GJ4Ct\\/hfZ1c3UNHw5bRwHRM0L0+6QYANTrPzl2X6hZK3kijlJsub+RvcPNPNQGrhK3e4yYHJmspW19qE5mPgxd5lbwzLfC4rOa2XJGXs8Am8hxBVUQBrYaSX5y1D\\/H2H+\\/KuPjUhMtylfH4pqvrYmedw8h56zQLScQQ1xOMsiYtb72YRegl4pByfwExmmQ3L8EtBRDGoJznbwnCe863BRgZTCS9jQT0Wry6f1UGhpmH98UCfP\\/fzWCLOCPaXJCH5gxYdSIOc7u4nw7mBbPk\\/xhFWz7PDTCw9wxEwVpLBshqbxVfPQ9eTND\\/BEd9hrtE\\/ZVlJz+wIIaabOUgyMMEqGqUNPvI5Dt6JLD\\/s2yPA2zd8saGSjrLcBHzKfKEt4prtCjasz+\\/IK8eWT5QCrrJC9swLAAdUFKjX6mAcpR0ZF97ubI6I4rheTkfhfQ+5gX9Dm7ahfs6b4Fzk0ewwY9uim4BEVkzQqHeIejtVShVG8LoXuqqPsen4YS2QGhDvzfop6Usr4J8Eb\\/lFZREasEN1MRNC8FqcQoWQPc\\/BGyxU0viDeZKH3wtZ2jXhs7l8xqX9jbaON1nqgdayLVLQz+POAZnQz7iwTjrFX9A9mYM\\/NgUA32jQq"}},"ver":"5.7.22","slot":[{"loid":"1,13,16,23","channel":"news_news_sh","recent_rot":["1,2,3","4,5,6","4,7,6","8","9,10"],"refresh_type":2,"islocal":1,"seq_loid":"1,1,1,1,1","cur":58,"orders_info":["91987701,8122038,0,19,4201,110,1","91234216,9163219,537783065,19,4301,110,1","89033789,1252416,860268798,19,2804,110,1","91741757,7748442,3134180117,19,4301,110,1","82330504,3792964,3640746586,1000,101,110,2","89123229,8862275,3031448928,19,507,110,1","91890078,8651597,3360875772,19,4001,110,3","91961141,7311564,2009512134,19,4301,110,1","91639391,9117817,826327870,1000,4201,110,2","76056706,6378696,2577335544,19,4107,110,1"],"current_rot":"4,7,6,8,9","seq":"4,9,15,40,50"}],"appversion":"181210"}',
                'lc_ids': 'CELLSHC201504210000',
                'uid': '6F0D5898-2C3A-46FE-9181-589BC52ED743',
                'is_new_user': '0',
                #                    'feedbackNewsId': '20181228A0XN2100|2|0,20181215V0I04Q00|4|0,20181228A15XAT00|2|0,20181224V0C58S00|4|0,20181228A1EQLH00|2|0',
                'chlid': channel_id,
                'channelType': channel_id,
                'newsTopPage': '0',
                'feedbackCur': '53',
                'channelPosition': '67',
                'page': '1',
                'forward': '1',
                'picType': '1,2,0,2,1,2,1,2,1,0,1,2,0,2,1,2,1,2,1,2',
                'cachedCount': '50'}

        headers = {"content-type": "application/x-www-form-urlencoded",
                   "store": "1",
                   "accept": "*/*",

                   "idft": "CE1E8744-7BF9-4FDD-87A5-463C6B9A66E1",
                   "idfa": "05571C2D-1C86-4B5B-87EF-E4B4DAF07DDB",
                   "appver": "12.0.1_qqnews_5.7.22",

                   "devid": "d605a70a-d084-487e-aaf1-8a057d40ef39",
                   "devicetoken": "<f4b49138 3ca95e38 1519836e daefaab6 799b04da c164f7a7 4cb7d999 6e343393>",
                   "accept-language": "zh-Hans-CN;q=1, en-CN;q=0.9",
                   "referer": "http://inews.qq.com/inews/iphone/",
                   "user-agent": "QQNews/5.7.22 (iPhone; iOS 12.0.1; Scale/2.00)",
                   "content-length": "2169",
                   "accept-encoding": "br, gzip, deflate",
                   "cookie": "logintype=2",
                   "qqnetwork": "wifi"}

        domain_url = "https://r.inews.qq.com/getQQNewsUnreadList?"

        query_dict = {'appver': '12.0.1_qqnews_5.7.22',
                      'pagestartfrom': 'icon',
                      'page_type': 'timeline',
                      'apptype': 'ios',
                      'rtAd': '1',
                      'imsi': '460-01',
                      'screen_height': '667',

                      'network_type': 'wifi',
                      'startTimestamp': '1545835451',
                      'store': '1',
                      'deviceToken': '<f4b49138 3ca95e38 1519836e daefaab6 799b04da c164f7a7 4cb7d999 6e343393>',
                      'global_info': '1|1|1|1|1|14|4|1|0|6|1|1|2|2|0|J267P000000000:J060P000000000:B054P000015802:J054P600000000|1421|0|1|0|0|0|0|0|1001|0|6|1|1|1|1|1|1|-1|0|0|0|2|1|1|0|0|2|0|1|0|4|0|0|0|3|0|0|0|0',
                      'globalInfo': '1|1|1|1|1|14|4|1|0|6|1|1|2|2|0|J267P000000000:J060P000000000:B054P000015802:J054P600000000|1421|0|1|0|0|0|0|0|1001|0|6|1|1|1|1|1|1|-1|0|0|0|2|1|1|0|0|2|0|1|0|4|0|0|0|3|0|0|0|0',
                      'screen_scale': '2',
                      'activefrom': 'icon',
                      'screen_width': '375',
                      'isJailbreak': '0',

                      'qqnews_refpage': 'QNCommonListController',
                      'omgid': 'a305486b92cc9e48f90929497de4cb30dfde0010112206',
                      'device_model': 'iPhone9,1',
                      'pagestartFrom': 'icon',
                      'device_appin': '6F0D5898-2C3A-46FE-9181-589BC52ED743',
                      'devid': 'D605A70A-D084-487E-AAF1-8A057D40EF39',
                      'omgbizid': '138dc6ef3ae8a24f7c897a9bbde8b9098f210060113210',
                      'idfa': '05571C2D-1C86-4B5B-87EF-E4B4DAF07DDB'}
        count = 0
        result_list = []
        while count < list_page_max:
            post_dic['newsTopPage'] = str(count)
            post_dic['page'] = str(count + 1)
            timestamp = int(time.time())
            query_dict['startTimestamp'] = timestamp
            url = domain_url + urllib.parse.urlencode(query_dict)
            get_page = requests.post(url, data=post_dic, headers=headers)
            page_dict = get_page.json()
            page_list.append(page_dict)
            count += 1
            #            continue
            # video_list1 = page_dict["kankaninfo"]["videos"]
            video_list2 = page_dict["newslist"]
            count += 1
            if video_list2 != []:
                print("get data at page %s" % str(count - 1))
            for video_info in video_list2:
                has_video = video_info.get('hasVideo')
                video_channel = video_info.get('video_channel')
                if has_video == 1 or video_channel is not None:
                    video_dict = copy.deepcopy(self.video_data)
                    video_dict['channel'] = channel_id
                    video_dict['title'] = video_info['title']
                    print(video_dict['title'])
                    video_dict['url'] = video_info['url']
                    try:
                        dura_str = video_info['video_channel']['video']['duration']
                        video_dict['duration'] = trans_duration(dura_str)
                    except:
                        video_dict['duration'] = 0
                    try:
                        video_dict['releaser'] = video_info['chlname']
                    except:
                        video_dict['releaser'] = None
                    try:
                        video_dict['releaser_id'] = video_info['card']['uin']
                    except:
                        video_dict['releaser_id'] = None
                    video_dict['release_time'] = int(video_info['timestamp'] * 1e3)
                    try:
                        video_dict['read_count'] = video_info['readCount']
                    except:
                        video_dict['read_count'] = 0
                    video_dict['comment_count'] = video_info['comments']
                    video_dict['favorite_count'] = video_info['likeInfo']
                    try:
                        video_dict['play_count'] = video_info['video_channel']['video']['playcount']
                    except:
                        video_dict['play_count'] = 0
                    video_dict['article_id'] = video_info['id']
                    try:
                        video_dict['video_id_str'] = video_info['vid']
                    except:
                        video_dict['video_id_str'] = None
                    video_dict['fetch_time'] = int(time.time() * 1e3)
                    result_list.append(video_dict)
                    if len(result_list) >= 100:
                        output_result(result_Lst=result_list,
                                      platform=self.platform,
                                      output_to_file=output_to_file,
                                      filepath=filepath,
                                      output_to_es_raw=output_to_es_raw,
                                      es_index=es_index,
                                      doc_type=doc_type,
                                      output_to_es_register=output_to_es_register)
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
            return result_list

    def releaser_page_by_time(self, start_time, end_time, url,allow,**kwargs):
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
                        yield res
                else:
                    count_false += 1
                    if count_false > allow:
                        break
                    else:
                        yield res
if __name__ == "__main__":
    t = Crawler_Tencent_News()
    #t.get_releaser_follower_num("https://view.inews.qq.com/media/5498518?tbkt=I&uid=")
    url = "https://view.inews.qq.com/media/5196832"
    # t.releaser_page("https://view.inews.qq.com/media/5196832",output_to_es_raw=True, es_index='crawler-data-raw', doc_type='doc')
    t.releaser_page_by_time(1546272000000, 1564362018000, url, output_to_es_raw=True,
                               es_index='crawler-data-raw',
                               doc_type='doc', releaser_page_num_max=4000)
