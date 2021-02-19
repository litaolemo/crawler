# -*- coding:utf-8 -*-
# @Time : 2020/2/26 11:40
# @Author : litao

import requests
import json, re, datetime, urllib
from crawler.crawler_sys.utils.output_results import retry_get_url
from crawler.crawler_sys.utils.output_results import hot_words_output_result, output_result
from crawler.crawler_sys.utils.trans_strtime_to_timestamp import trans_strtime_to_timestamp
from write_data_into_es.func_cal_doc_id import *
from crawler.crawler_sys.site_crawler.crawler_haokan import Crawler_haokan
crawler_video_page = Crawler_haokan().video_page


class CrawlerHaoKan(object):
    def __init__(self):
        self.platform = "haokan"
        self.headers = {
                "Host": "sv.baidu.com",
                "Connection": "keep-alive",

                "Charset": "UTF-8",
                "User-Agent": "Mozilla/5.0 (Linux; Android 5.1.1; OPPO R11 Build/NMF26X; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/74.0.3729.136 Mobile Safari/537.36 haokan/5.9.2.10 (Baidu; P1 5.1.1)/OPPO_22_1.1.5_11R+OPPO/1022131c/3B42DEA1B123E0BFCC96D85E1E191EB1%7C0/1/5.9.2.10/509021/1",
                "X-Bfe-Quic": "enable=1",
                # "XRAY-REQ-FUNC-ST-DNS": "okHttp;1582687757091;0",
                # "XRAY-TRACEID": "58f10e39-772a-42b0-bed2-451038d27de4",
                # "Cookie": "BAIDUID=E577F98F951CE0989D45142695B6CE78:FG=1; FEED_VIDS=8633+8523+6577+3630; FEED_TAB=recommend; BAIDUZID=FFD42183BD34A7D8D951D8D356B53F7BBC; BAIDUCUID=_82ZiliKS8lNav8m0aHRuliP-i0EOvatgiv6fg8kSiKoLqqqB",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept-Encoding": "gzip, deflate",

        }
        self.get_hot_page_videos()

    def get_hot_words(self):
        bulk_list = []
        timestamp = int(datetime.datetime.now().timestamp())
        url_dic = {
                "log": "vhk",
                "tn": "1022131c",
                "ctn": "1022131c",
                # "mac": "48:A4:72:58:86:D5",
                # "imei": "866174725888628",
                # "cuid": "3B42DEA1B123E0BFCC96D85E1E191EB1|0",
                "bdboxcuid": "",
                # "c3_aid": "A00-GH4F2VNIUV7SHQU3HMUUFTLTKSN3IUAD-ZURJH6Y5",
                "os": "android",
                "osbranch": "a0",
                "ua": "900_1600_320",
                "ut": "OPPO R11_5.1.1_22_OPPO",
                "uh": "OPPO,qcom,sdm660",
                "apiv": "5.9.2.10",
                "appv": "509021",
                "version": "5.9.2.10",
                "life": timestamp,
                "clife": timestamp,
                # "hid": "3B691F5D047A9200FADD7D5BA67D1B78",
                "imsi": "0",
                "network": "1",
                # "location": r"{%22prov%22:%22%22,%22city%22:%22%22,%22county%22:%22%22,%22street%22:%22%22,%22latitude%22:30.004828,%22longitude%22:112.575499}",
                "sids": "5155_2",
                "young_mode": "0",
        }
        post_dic = {
                "search/presug": "method=get"
        }
        url = "https://sv.baidu.com/haokan/api?%s" % urllib.parse.urlencode(url_dic)
        res = requests.post(url, data=post_dic, headers=self.headers)
        res_json = res.json()
        for data_list in res_json["search/presug"]["data"]:
            for data in data_list["list"]:
                try:
                    dic = {
                            "platform": self.platform,
                            "title": data["title"],
                            "cmd": data["cmd"],
                            "fetch_time": int(datetime.datetime.now().timestamp() * 1e3),
                            "data_type": data['tplName']
                    }
                    bulk_list.append(dic)
                except:
                    continue
        hot_words_output_result(bulk_list)
        return True

    def search_hottopic(self,title=None,url="",channel="",max_page=2,**kwargs):
        """
        GET https://haokan.baidu.com/creator/haokantopic/topichomepage/5875485993792393513&sfrom=inside-souqianyeHuaTi?id=5875485993792393513&bfe=1 HTTP/1.1
        :param url: 
        :return: 
        """
        res_data_list= []
        url = urllib.parse.unquote(url)
        _id = re.findall("/(\d+)&", url)[0]
        print(url, _id)
        get_url = re.findall("url_key=(.*)",url)[0] +"?id=%s&bfe=1" %_id
        headers = {
                "Host": "haokan.baidu.com",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Linux; Android 5.1.1; OPPO R11 Build/NMF26X; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/74.0.3729.136 Mobile Safari/537.36 haokan/5.9.2.10 (Baidu; P1 5.1.1)/OPPO_22_1.1.5_11R+OPPO/1022131c/3B42DEA1B123E0BFCC96D85E1E191EB1%7C0/1/5.9.2.10/509021/1",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
                # "Cookie": "BAIDUCUID=_82ZiliKS8lNav8m0aHRuliP-i0EOvatgiv6fg8kSiKoLqqqB; Hm_lvt_77ca61e523cd51ec7ac7a23bc4d24edf=1582689757; BAIDUZID=xOiExF1dC2xpt9E7juy1SeKlPDFxsf9nSi9w-06_uAg6ipAHsDqeeoUY6agIwPr6xlNET9po9osj0tENM2pCIZQ; Hm_lpvt_77ca61e523cd51ec7ac7a23bc4d24edf=1582799564; BAIDUID=E577F98F951CE0989D45142695B6CE78:FG=1",
                "X-Requested-With": "com.baidu.haokan",
        }

        res = retry_get_url(get_url,proxies=3,headers=headers,timeout=10)

        res_json = json.loads(re.findall(r"__PRELOADED_STATE__ = (.*);[ ]+document", res.text)[0])
        # print(res_json)
        for data in res_json["listData"]["list"]:
            res_data = crawler_video_page("https://haokan.baidu.com/v?vid=%s"%data.get("vid"), vid=data.get("vid"))
            res_data["channel"] = channel
            res_data["is_hot"] = 0
            res_data_list.append(res_data)
        page = 2
        while page <= max_page:
            headers["Referer"] = get_url
            get_url = "https://haokan.baidu.com/creator/topicvlist?id=%s&tabType=hot&page=%s&size=10" %(_id,page)
            page += 1
            res = retry_get_url(get_url,proxies=3,headers=headers,timeout=10)
            res_json = res.json()
            for data in res_json["data"]["list"]:
                res_data = crawler_video_page("https://haokan.baidu.com/v?vid=%s" % data.get("vid"),
                                              vid=data.get("vid"))
                res_data["channel"] = channel
                res_data["is_hot"] = 0
                res_data_list.append(res_data)

        output_result(result_Lst=res_data_list,
                      platform=self.platform,
                      output_to_es_raw=True,
                      )
        res_data_list.clear()


    def search_video(self, title=None, max_page=2,**kwargs):
        # 搜索页
        page = 1
        bulk_all_body = ""
        res_data_list = []
        timestamp = int(datetime.datetime.now().timestamp())
        while page <= max_page:
            url_dic = {
                    "cmd": "search",
                    "log": "vhk",
                    "tn": "1022131c",
                    "ctn": "1022131c",
                    # "mac": "48:A4:72:58:86:D5",
                    # "imei": "866174725888628",
                    # "cuid": "3B42DEA1B123E0BFCC96D85E1E191EB1|0",
                    "bdboxcuid": "",
                    "c3_aid": "A00-GH4F2VNIUV7SHQU3HMUUFTLTKSN3IUAD-ZURJH6Y5",
                    "os": "android",
                    "osbranch": "a0",
                    "ua": "900_1600_320",
                    "ut": "OPPO%20R11_5.1.1_22_OPPO",
                    "uh": "OPPO,qcom,sdm660",
                    "apiv": "5.9.2.10",
                    "appv": "509021",
                    "version": "5.9.2.10",
                    "life": timestamp,
                    "clife": timestamp,
                    "hid": "3B691F5D047A9200FADD7D5BA67D1B78",
                    "imsi": "0",
                    "network": "1",
                    # "location": "{%22prov%22:%22%22,%22city%22:%22%22,%22county%22:%22%22,%22street%22:%22%22,%22latitude%22:30.004828,%22longitude%22:112.575499}",
                    "sids": "5155_2",
                    "young_mode": "0",

            }
            post_dic = {
                    "method": "get",
                    "tag": "rc",
                    "cursor_time": "0",
                    "cb_cursor": "0",
                    "hot_cursor": "0",
                    "offline_cursor": "0",
                    "rn": "10",
                    "pn": str(page),
                    "title": title,
                    "force": "0",
                    "needBjh": "1",
                    "long_video": "1",
                    "wordseg": "1",
                    "outpn": "0",
                    "innerpn": "1",

            }
            post_body = {
                    "search": urllib.parse.urlencode(post_dic)
            }
            requests_res = requests.post("https://sv.baidu.com/haokan/api?%s" % urllib.parse.urlencode(url_dic),
                                         data=post_body, headers=self.headers)
            requests_json = requests_res.json()
            page += 1
            print(requests_json)
            for count, data in enumerate(requests_json["search"]["data"]["list"]):
                res_data = crawler_video_page(data.get("video_short_url"), data.get("media_id"))
                # res_data["hot_num"] = data.get("hot")
                res_data["is_hot"] = 0
                res_data_list.append(res_data)

            output_result(result_Lst=res_data_list,
                          platform=self.platform,
                          output_to_es_raw=True,
                          )
            res_data_list.clear()

    def get_hot_page_videos(self, max_page=10):

        res_data_list = []
        headers = {
                "Host": "haokan.baidu.com",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Linux; Android 5.1.1; OPPO R11 Build/NMF26X; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/74.0.3729.136 Mobile Safari/537.36 haokan/5.9.2.10 (Baidu; P1 5.1.1)/OPPO_22_1.1.5_11R+OPPO/1022131c/3B42DEA1B123E0BFCC96D85E1E191EB1%7C0/1/5.9.2.10/509021/1",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
                # "Cookie": "BAIDUCUID=_82ZiliKS8lNav8m0aHRuliP-i0EOvatgiv6fg8kSiKoLqqqB; Hm_lvt_77ca61e523cd51ec7ac7a23bc4d24edf=1582689757; BAIDUZID=xOiExF1dC2xpt9E7juy1SeKlPDFxsf9nSi9w-06_uAg4fX1n0Ludwa0A0JsRzcPL1pUlarSep8D1-W4SE8U4K9A; BAIDUID=E577F98F951CE0989D45142695B6CE78:FG=1; Hm_lpvt_77ca61e523cd51ec7ac7a23bc4d24edf=1582769261",
                "X-Requested-With": "com.baidu.haokan",
        }
        url = "https://haokan.baidu.com/haokan/wisehotbroadcast?sfrom=inside-search_found"
        res = retry_get_url(url=url, proxies=3, headers=headers)

        res_json = json.loads(re.findall(r"__PRELOADED_STATE__ = (.*);[ ]+document", res.text)[0])
        print(res_json)
        for data in res_json["video"]:
            res_data = crawler_video_page(data.get("pageUrl"), vid=data.get("vid"))
            res_data["hot_num"] = data.get("hot")
            res_data["is_hot"] = 1
            res_data_list.append(res_data)

        output_result(result_Lst=res_data_list,
                      platform=self.platform,
                      output_to_es_raw=True,
                      )
        res_data_list.clear()

    def get_hot_videos(self,**kwargs):
        if kwargs.get("data_type") == "hottopic_image":
            self.search_hottopic(**kwargs)
        else:
            self.search_video(**kwargs)


if __name__ == "__main__":
    crawler = CrawlerHaoKan()
    # crawler.get_hot_words()
    # crawler.get_hot_videos()
    crawler.search_hottopic("baiduhaokan://webview/?url_key=https%3A%2F%2Fhaokan.baidu.com%2Fcreator%2Fhaokantopic%2Ftopichomepage%2F5875485993792393513%26sfrom%3Dinside-souqianyeHuaTi",channel="#专治不开心#")