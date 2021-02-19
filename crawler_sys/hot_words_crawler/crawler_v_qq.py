# -*- coding:utf-8 -*-
# @Time : 2020/2/28 12:09 
# @Author : litao


import requests
import json, re, datetime, urllib
from crawler.crawler_sys.utils.output_results import retry_get_url
from crawler.crawler_sys.utils.output_results import hot_words_output_result, output_result
from crawler.crawler_sys.utils.trans_strtime_to_timestamp import trans_strtime_to_timestamp
from write_data_into_es.func_cal_doc_id import *
from lxml import etree
from crawler.crawler_sys.site_crawler.crawler_v_qq import Crawler_v_qq as Crawler_qq
crawler_qq_video_page = Crawler_qq().video_page

class Crawler_v_qq(object):
    def __init__(self):
        self.platform = "腾讯视频"
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

    def get_hot_words(self):
        bulk_list = []
        timestamp = int(datetime.datetime.now().timestamp())
        url = "https://v.qq.com/biu/ranks/?t=hotsearch&channel=hot"
        headers = {

                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "accept-encoding": "gzip, deflate",
                "accept-language": "zh,zh-CN;q=0.9",
                "cache-control": "max-age=0",
                # "cookie": "pgv_pvi=3517925376; pgv_pvid=3591400976; RK=sDRQYhGkF/; ptcz=8100687e80e810853d573a8a9ced1155a9a9683321075161f61b773de19ff4c5; pac_uid=0_bf3968e8e3157; ts_uid=1260359885; tvfe_boss_uuid=082fecb8ba01b06d; QQLivePCVer=50181223; video_guid=ce0aa0f8275ad435; video_platform=2; bucket_id=9231001; mobileUV=1_1707c108811_53c13; tvfe_search_uid=3c2fd48b-03f8-4f63-af8c-bb2bd367af2b; ts_refer=www.baidu.com/link; ad_play_index=71; pgv_info=ssid=s7741803072; ts_last=v.qq.com/biu/ranks/",
                "if-modified-since": "Fri, 28 Feb 2020 07:10:00 GMT",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36",
        }

        page_res = retry_get_url(url, headers=headers, proxies=3, timeout=5)
        page_text = page_res.content.decode("utf-8")
        html = etree.HTML(page_text)
        print(html)
        xpath_list = html.xpath("//ul[@class='table_list']/li")
        for li in xpath_list:
            title = li.xpath("./div[1]/a/@title")
            title_url = li.xpath("./div[1]/a/@href")
            if title:
                dic = {
                        "platform": self.platform,
                        "title": title[0],
                        "fetch_time": int(datetime.datetime.now().timestamp() * 1e3),
                        "url":title_url[0]
                }
                bulk_list.append(dic)
        hot_words_output_result(bulk_list)
        return True

    def get_hot_videos(self,url="", max_page=10,**kwargs):
        data_list = []
        headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "accept-encoding": "gzip, deflate",
                "accept-language": "zh,zh-CN;q=0.9",
                "cache-control": "max-age=0",
                # "cookie": "pgv_pvi=3517925376; pgv_pvid=3591400976; RK=sDRQYhGkF/; ptcz=8100687e80e810853d573a8a9ced1155a9a9683321075161f61b773de19ff4c5; pac_uid=0_bf3968e8e3157; ts_uid=1260359885; tvfe_boss_uuid=082fecb8ba01b06d; QQLivePCVer=50181223; video_guid=ce0aa0f8275ad435; video_platform=2; bucket_id=9231001; mobileUV=1_1707c108811_53c13; tvfe_search_uid=3c2fd48b-03f8-4f63-af8c-bb2bd367af2b; ts_refer=www.baidu.com/link; pgv_info=ssid=s7741803072; ad_play_index=80",
                # "if-modified-since": "Fri, 28 Feb 2020 08:00:00 GMT",
                "referer": "https://v.qq.com/biu/ranks/?t=hotsearch&channel=hot",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
        }
        res = retry_get_url(url,headers=headers,timeout=10,proxies=3)
        page_text = res.content.decode("utf-8")
        html = etree.HTML(page_text)
        print(html)
        xpath_list = html.xpath("//body[@class='page_search']/div[@class='search_container']/div[@class='wrapper']/div[@class='wrapper_main']/div")
        for li in xpath_list:
            title_url = li.xpath("./a/@href")
            if title_url:
                print(title_url)
                data = crawler_qq_video_page(title_url[0])
                if not data:
                    continue
                data["is_hot"] = 1
                data_list.append(data)
        output_result(result_Lst=data_list,
                      platform=self.platform,
                      output_to_es_raw=True,
                      )
        data_list.clear()

if __name__ == "__main__":
    crawler = Crawler_v_qq()
    # crawler.get_hot_words()
    crawler.get_hot_videos("https://v.qq.com/x/search/?q=%E6%95%99%E8%82%B2%E9%83%A8%E5%9B%9E%E5%BA%94%E6%89%A9%E5%A4%A7%E7%A1%95%E5%A3%AB%E5%92%8C%E4%B8%93%E5%8D%87%E6%9C%AC%E6%8B%9B%E7%94%9F&stag=12",channel="教育部回应扩大硕士和专升本招生")