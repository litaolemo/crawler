# -*- coding:utf-8 -*-
# @Time : 2020/3/4 18:26 
# @Author : litao
# -*- coding:utf-8 -*-
# # @Time : 2020/3/4 10:33
# # @Author : litao


import requests
import json, re, datetime, urllib
from crawler.crawler_sys.utils.output_results import retry_get_url
from crawler.crawler_sys.utils.output_results import hot_words_output_result, output_result
from crawler.crawler_sys.utils.trans_strtime_to_timestamp import trans_strtime_to_timestamp
from crawler.crawler_sys.utils.trans_duration_str_to_second import trans_duration
from crawler.crawler_sys.utils.trans_str_play_count_to_int import trans_play_count
from write_data_into_es.func_cal_doc_id import *
import base64
from urllib.parse import parse_qs, urlparse


class Crawler_kwai(object):
    def __init__(self):
        self.platform = "kwai"
        timestamp = int(datetime.datetime.now().timestamp() * 1e6)
        self.headers = {

                "Host": "apissl.ksapisrv.com",
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
                "Connection": "keep-alive",
                # "X-REQUESTID": str(timestamp),
                "Cookie": "region_ticket=RT_587AF3F9478D42A3D5F60AB62CE1C0E0EA160BE094A87D6B237E9D5FD95A0",
                "User-Agent": "kwai-ios",
                "Accept-Language": "zh-Hans-CN;q=1",
                "Accept-Encoding": "gzip, deflate",

        }

    def get_hot_words(self):
        bulk_list = []

        url = "https://apissl.ksapisrv.com/rest/n/search/home/hot?kcv=188&kpf=IPAD&net=_5&appver=7.1.2.1527&kpn=KUAISHOU&mod=iPad6%2C11&c=a&sys=ios13.3.1&sh=2048&ver=7.1&isp=&did=8DE774CA-8F57-488F-8889-79D60AEDE388&ud=1815100604&browseType=1&sw=1536&egid=DFPF406D78B30ED8A082E3613DB9F5E4EA35664F0527B2BE54F7719A7A61C90B"
        data_dic = {

                "__NS_sig3": "2207476318d84d65428e1871c0a2cfd5dd6736e1f2",
                "__NStokensig": "d54ac0ce2436a4a265c9542af6c39952963badf5716d339f11ab541c2cb4e34c",
                "client_key": "56c3713c",
                "country_code": "cn",
                "global_id": "DFPF406D78B30ED8A082E3613DB9F5E4EA35664F0527B2BE54F7719A7A61C90B",
                "kuaishou.api_st": "Cg9rdWFpc2hvdS5hcGkuc3QSoAGx9VGkywy5Mvl97AA_lk1kfdCLTA0X61478qJ0Kb3fKEpZBlNM-jYcJ52ydyEaBpzbWAy7QEed0FcZ5t9zUltbXHaviXuUq0FiZkbuJXUs3md9v3Iro_NQ9NYWn7pzyc59CERXsLzM6G3N3DH-vUt8qT9Mg-HAAaLZngM_7UlQxkrlNmb4k9AJnz0BjxcXSK7FbQgde-U1B6Gt1VQFTLZHGhJrNN-fL8lNH4AKLNFPdkh-L7kiIAXhY7DWXa-tqY6EzPQmA3ZVd0M8b60rSf2LVxxji-LqKAUwAQ",
                "language": "zh-Hans-CN;q=1",
                "sig": "84bf503fa751ef6b33f7506258298170",
                "token": "dd79d261eeec4ef1b0370d07691a8122-1815100604",

        }
        page_res = requests.post(url, headers=self.headers, data=data_dic)
        page_json = page_res.json()
        for data in page_json["hots"]:
            search_title = data["keyword"]
            if search_title:
                dic = {
                        "platform": self.platform,
                        "title": search_title,
                        "fetch_time": int(datetime.datetime.now().timestamp() * 1e3),

                }
                bulk_list.append(dic)
        hot_words_output_result(bulk_list)
        return True

    def search_page(self, title, *args):
        data_list = []
        timestamp = int(datetime.datetime.now().timestamp() * 1e8)
        headers = {

                "Host": "apissl.ksapisrv.com",
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
                "Connection": "keep-alive",
                "X-REQUESTID": "158332601966266545",
                "Cookie": "region_ticket=RT_A900E3AE922501DB6DED77DD16B0143B1099175ABCF9B34E20A04FED32A04",
                "User-Agent": "kwai-ios",
                "Accept-Language": "zh-Hans-CN;q=1",
                "Accept-Encoding": "gzip, deflate",

        }
        url = "https://apissl.ksapisrv.com/rest/n/search/feed?kcv=188&kpf=IPAD&net=_5&appver=7.1.2.1527&kpn=KUAISHOU&mod=iPad6%2C11&c=a&sys=ios13.3.1&sh=2048&ver=7.1&isp=&did=8DE774CA-8F57-488F-8889-79D60AEDE388&ud=1815100604&browseType=1&sw=1536&egid=DFPF406D78B30ED8A082E3613DB9F5E4EA35664F0527B2BE54F7719A7A61C90B"

        post_json = {
                "__NS_sig3": "2207493808d8530faf9fb99c7f0aa3339c090c83fa",
                "__NStokensig": "e54c8fcb82174761862807ce5ffbe8d18af9898fc5464105a71d4a58cf53a5aa",
                "client_key": "56c3713c",
                "country_code": "cn",
                "global_id": "DFPF406D78B30ED8A082E3613DB9F5E4EA35664F0527B2BE54F7719A7A61C90B",
                "isRecoRequest": "0",
                "keyword": title,
                "kuaishou.api_st": "Cg9rdWFpc2hvdS5hcGkuc3QSoAGx9VGkywy5Mvl97AA_lk1kfdCLTA0X61478qJ0Kb3fKEpZBlNM-jYcJ52ydyEaBpzbWAy7QEed0FcZ5t9zUltbXHaviXuUq0FiZkbuJXUs3md9v3Iro_NQ9NYWn7pzyc59CERXsLzM6G3N3DH-vUt8qT9Mg-HAAaLZngM_7UlQxkrlNmb4k9AJnz0BjxcXSK7FbQgde-U1B6Gt1VQFTLZHGhJrNN-fL8lNH4AKLNFPdkh-L7kiIAXhY7DWXa-tqY6EzPQmA3ZVd0M8b60rSf2LVxxji-LqKAUwAQ",
                "language": "zh-Hans-CN;q=1",
                # "sig": "f67fd34ff32ad20b8648f8013ba8cf14",
                "token": "dd79d261eeec4ef1b0370d07691a8122-1815100604",

        }
        # sig 为keyword 加密的字段 无法破解
        res = requests.post(url, headers=headers, data=post_json)
        page_text = res.json()
        for one_video in page_text["mixFeeds"]:
            video_dic = {}
            try:
                one_video = one_video["feed"]
                photoId_list = one_video.get('share_info').split("&")
                for photoid in photoId_list:
                    if "photoId=" in photoid:
                        photoid = photoid.replace("photoId=", "")
                    elif "userId=" in photoid:
                        releaser_id = photoid.replace("userId=", "")
                video_dic['video_id'] = photoid
                video_dic['title'] = one_video.get('caption')
                video_dic['url'] = "https://live.kuaishou.com/u/%s/%s" % (releaser_id, photoid)
                video_dic['releaser'] = one_video.get('user_name')
                video_dic['releaserUrl'] = 'https://live.kuaishou.com/profile/%s' % releaser_id
                release_time = int(one_video.get('timestamp'))
                video_dic['release_time'] = int(release_time)
                video_dic['video_id'] = photoid
                video_dic['duration'] = int(one_video.get("duration") / 1000)
                video_dic['play_count'] = one_video.get('view_count')
                video_dic['repost_count'] = one_video.get('share_count')
                video_dic['comment_count'] = one_video.get('comment_count')
                video_dic['favorite_count'] = one_video.get('like_count')
                video_dic['fetch_time'] = int(datetime.datetime.now().timestamp() * 1e3)
                video_dic['releaser_id_str'] = "kwai_%s" % releaser_id
                video_dic['platform'] = self.platform
                video_dic["is_hot"] = 1
                video_dic["data_provider"] = "CCR"
            except Exception as e:
                print(e)
                continue
            data_list.append(video_dic)
        output_result(result_Lst=data_list,
                      platform=self.platform,
                      output_to_es_raw=True,
                      )
        data_list.clear()

    def get_hot_videos(self,*args,**kwargs):
        pass
        # self.search_page(title, *args)


if __name__ == "__main__":
    crawler = Crawler_kwai()
    crawler.get_hot_words()
    crawler.search_page("王俊凯上网课")
