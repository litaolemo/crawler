# -*- coding:utf-8 -*-
# @Time : 2020/3/2 16:37 
# @Author : litao
# -*- coding:utf-8 -*-
# @Time : 2020/3/2 11:07
# @Author : litao
# -*- coding:utf-8 -*-
# @Time : 2020/2/28 12:09
# @Author : litao


import requests
import json, re, datetime, urllib
from crawler.crawler_sys.utils.output_results import retry_get_url
from crawler.crawler_sys.utils.output_results import hot_words_output_result, output_result
from crawler.crawler_sys.utils.trans_strtime_to_timestamp import trans_strtime_to_timestamp
from write_data_into_es.func_cal_doc_id import *



class Crawler_douyin(object):
    def __init__(self):
        self.platform = "抖音"
        timestamp = int(datetime.datetime.now().timestamp() * 1e3)
        self.headers = {

                "Host": "api3-normal-c-lf.amemv.com",
                "Connection": "keep-alive",
                # "Cookie": "d_ticket=38c841789e38ea43c6338910dac65ffe192e3; odin_tt=82086544bb9028f027b5aea78724ccf512dead26658f45321be33bade615793782bf6ac7fe0c18b73b9592f4284413d5300974810d439b42ef0b3eaa761b1640; msh=cakLg8lvbK5CxiSWkIbD2UInwAI; sid_guard=09fe3dfd89dfbc79f081fb2db9dd81ee%7C1581832192%7C5184000%7CThu%2C+16-Apr-2020+05%3A49%3A52+GMT; uid_tt=da0b53b7563eca87c47da41f5f17c30f; uid_tt_ss=da0b53b7563eca87c47da41f5f17c30f; sid_tt=09fe3dfd89dfbc79f081fb2db9dd81ee; sessionid=09fe3dfd89dfbc79f081fb2db9dd81ee; sessionid_ss=09fe3dfd89dfbc79f081fb2db9dd81ee; install_id=104847319549; ttreq=1$51e484720311469c4b70f4754d730d538a074c4b",
                # "X-SS-REQ-TICKET": "1583137750770",
                # "X-Tt-Token": "0009fe3dfd89dfbc79f081fb2db9dd81ee013243f7134b3eb37249cc729a5276172df69a4391b56ae4bf253c3c6352322611",
                "sdk-version": "1",
                "X-SS-DP": "1128",
                # "x-tt-trace-id": "00-9a5d00730a107b4310780861c7c50468-9a5d00730a107b43-01",
                "User-Agent": "com.ss.android.ugc.aweme/990 (Linux; U; Android 5.1.1; zh_CN; OPPO R11; Build/NMF26X; Cronet/77.0.3844.0)",
                "Accept-Encoding": "gzip, deflate",
                # "X-Gorgon": "0401c0cd4001df62dd7cff2a7d35092b14b3f2264163368f7f19",
                # "X-Khronos": "1583137750",

        }

    def get_hot_words(self):
        bulk_list = []
        url = "https://api3-normal-c-lf.amemv.com/aweme/v1/hot/search/list/?detail_list=1&mac_address=48%3AA4%3A72%3A58%3A86%3AD5&os_api=22&device_type=OPPO%20R11&ssmix=a&manifest_version_code=990&dpi=320&uuid=866174725888628&app_name=aweme&version_name=9.9.0&app_type=normal&ac=wifi&update_version_code=9902&channel=tengxun_new&device_platform=android&iid=104847319549&version_code=990&cdid=fce00742-ccef-4b14-943d-1f62b6d637b0&openudid=48a4725886d57203&device_id=70787469432&resolution=900*1600&os_version=5.1.1&language=zh&device_brand=OPPO&aid=1128&mcc_mnc=46007"
        page_res = retry_get_url(url, headers=self.headers, proxies=3, timeout=5)
        page_json = page_res.json()
        for data in page_json["data"]["word_list"]:
            title = data["word"]
            if title:
                dic = {
                        "platform": self.platform,
                        "title": title,
                        "fetch_time": int(datetime.datetime.now().timestamp() * 1e3),
                        "hot_value": data.get("hot_value"),
                        "top": data.get("position"),
                }
                bulk_list.append(dic)
        hot_words_output_result(bulk_list)
        return True

    def search_page(self, title=None,**kwargs):
        data_list = []
        headers = {

                "Host": "aweme.snssdk.com",
                "Connection": "keep-alive",
                # "Cookie": "d_ticket=38c841789e38ea43c6338910dac65ffe192e3; odin_tt=82086544bb9028f027b5aea78724ccf512dead26658f45321be33bade615793782bf6ac7fe0c18b73b9592f4284413d5300974810d439b42ef0b3eaa761b1640; msh=cakLg8lvbK5CxiSWkIbD2UInwAI; sid_guard=09fe3dfd89dfbc79f081fb2db9dd81ee%7C1581832192%7C5184000%7CThu%2C+16-Apr-2020+05%3A49%3A52+GMT; uid_tt=da0b53b7563eca87c47da41f5f17c30f; uid_tt_ss=da0b53b7563eca87c47da41f5f17c30f; sid_tt=09fe3dfd89dfbc79f081fb2db9dd81ee; sessionid=09fe3dfd89dfbc79f081fb2db9dd81ee; sessionid_ss=09fe3dfd89dfbc79f081fb2db9dd81ee; install_id=104847319549; ttreq=1$51e484720311469c4b70f4754d730d538a074c4b",
                # "X-SS-REQ-TICKET": "1583139618192",
                # "X-Tt-Token": "0009fe3dfd89dfbc79f081fb2db9dd81ee013243f7134b3eb37249cc729a5276172df69a4391b56ae4bf253c3c6352322611",
                "sdk-version": "1",
                # "x-tt-trace-id": "00-9a797f160a107b431078db3e93480468-9a797f160a107b43-01",
                "User-Agent": "com.ss.android.ugc.aweme/990 (Linux; U; Android 5.1.1; zh_CN; OPPO R11; Build/NMF26X; Cronet/77.0.3844.0)",
                "Accept-Encoding": "gzip, deflate",
                # "X-Gorgon": "0401a0514001f64964a8ebef9f4305ccbef2df1aa3c92fdf955a",
                # "X-Khronos": "1583139618",

        }
        url = "https://aweme.snssdk.com/aweme/v1/hot/search/video/list/?hotword={0}&offset=0&count=12&source=trending_page&is_ad=0&item_id_list&is_trending=0&os_api=22&device_type=OPPO%20R11&ssmix=a&manifest_version_code=990&dpi=320&uuid=866174725888628&app_name=aweme&version_name=9.9.0&ts=1583139619&app_type=normal&ac=wifi&update_version_code=9902&channel=tengxun_new&_rticket=1583139618192&device_platform=android&iid=104847319549&version_code=990&cdid=fce00742-ccef-4b14-943d-1f62b6d637b0&openudid=48a4725886d57203&device_id=70787469432&resolution=900*1600&os_version=5.1.1&language=zh&device_brand=OPPO&aid=1128&mcc_mnc=46007".format(title)
        res = retry_get_url(url, headers=headers, timeout=5, proxies=3)
        page_text = res.json()
        for one_video in page_text["aweme_list"]:
            video_dic = {}
            video_dic['title'] = one_video.get('desc')
            video_dic['url'] = one_video.get('share_url')
            releaser_id = one_video.get('author_user_id')
            video_dic['releaser'] = one_video.get('author').get("nickname")
            video_dic['releaserUrl'] = "https://www.iesdouyin.com/share/user/%s" % releaser_id
            release_time = one_video.get('create_time')
            video_dic['release_time'] = int(release_time * 1e3)
            video_dic['duration'] = int(one_video.get('duration') / 1000)
            video_dic['play_count'] = 0
            video_dic['repost_count'] = one_video.get('statistics').get('share_count')
            video_dic['comment_count'] = one_video.get('statistics').get('comment_count')
            video_dic['favorite_count'] = one_video.get('statistics').get('digg_count')
            video_dic['video_id'] = one_video.get('aweme_id')
            video_dic['fetch_time'] = int(datetime.datetime.now().timestamp() * 1e3)
            video_dic['releaser_id_str'] = "抖音_%s" % releaser_id
            video_dic['platform'] = "抖音"
            video_dic['video_img'] = one_video.get('video').get('cover').get('url_list')[0]
            video_dic["is_hot"] = 1
            video_dic["data_provider"] = "CCR"
            data_list.append(video_dic)
        output_result(result_Lst=data_list,
                      platform=self.platform,
                      output_to_es_raw=True,
                      )
        data_list.clear()
        ## sign和ts为加密字段 无法解决

    def get_hot_videos(self,*args,**kwargs):
        self.search_page(**kwargs)


if __name__ == "__main__":
    crawler = Crawler_douyin()
    crawler.get_hot_words()
    crawler.search_page("模仿刘柏辛的哼翻车")
