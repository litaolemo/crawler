# -*- coding:utf-8 -*-
# @Time : 2019/6/10 19:07
# @Author : litao
import time, datetime, random
import pandas as pd
import os
import re
import copy
import json
import time, datetime
import requests
import urllib
from urllib import parse
from urllib.parse import urlencode


try:
    from crawler_sys.site_crawler.func_get_releaser_id import *
except:
    from crawler.func_get_releaser_id import *


class Craler_toutiao(object):
    def get_releaser_id(self, releaserUrl):
        return get_releaser_id(platform="toutiao", releaserUrl=releaserUrl)

    def releaser_page(self, releaserUrl,
                          output_to_file=False,
                          filepath=None,
                          releaser_page_num_max=10000,
                          output_to_es_raw=False,
                          output_to_es_register=False,
                          push_to_redis=False,
                          es_index=None,
                          doc_type=None,
                          proxy_dic=None):

        result_list = []
        has_more = True
        count = 1
        releaser_id = self.get_releaser_id(releaserUrl)

        headers = {
                "Accept-Encoding": "gzip",
                "X-SS-REQ-TICKET": str(int(datetime.datetime.now().timestamp() * 1e3)),
                "sdk-version": "1",
                "Cookie": "odin_tt=64f2d5f2a9d2cb38242f3dd7345d0ee58710aaa3c954116af09471e4b35c38a5b0cc74c258cdf4f1989e4559d3d52c9a3ddd23eecd146d6df7fb0918bd76f272; qh[360]=1; UM_distinctid=16a2f3efde2c8-0242614eaeff-3d4a4679-64140-16a2f3efde341; CNZZDATA1264530760=1774845088-1555569575-%7C1555569575",
                "X-Gorgon": "0300000040018b4ce801b68de02a3cb11582f701e76f75a0f27a",
                "X-Khronos": str(int(datetime.datetime.now().timestamp())),
                "X-Pods": "",
                "Host": "ic-hl.snssdk.com",
                "Connection": "Keep-Alive",
                "User-Agent": "okhttp/3.10.0.1"

        }

        offset = "0"
        while has_more and count <= releaser_page_num_max:
            # print(str(releaser_id)+str(max_behot_time))
            # js_head = json.loads(get_js(str(releaser_id)+str(max_behot_time)))
            url_dic = {"category": "profile_video",
                       "visited_uid": releaser_id,
                       "stream_api_version": "88",
                       "count": "20",
                       "offset": offset,
                       "client_extra_params": '{"playparam": "codec_type:0"}',
                       # "iid": "77625756096",
                       "device_id": "67315358179",
                       "ac": "wifi",
                       "channel": "update",
                       "aid": "13",
                       "app_name": "news_article",
                       "version_code": "732",
                       "version_name": "7.3.2",
                       "device_platform": "android",
                       "ab_version": "739390,662099,668774,765190,976875,857803,952277,757281,679101,660830,830855,947965,942635,662176,665176,674051,643894,919834,649427,677130,710077,801968,707372,661900,668775,990369,759657,661781,648315",
                       "ab_group": "100168",
                       "ab_feature": "94563,102749",
                       "ssmix": "a",
                       "device_type": "oppo R11s Plus",
                       "device_brand": "OPPO",
                       "language": "zh",
                       "os_api": "23",
                       "os_version": "7.0.1",
                       #"uuid": "250129616283002",
                       #"openudid": "7313ae71df9e5367",
                       # "_rticket": "1562212986716",
                       "manifest_version_code": "731",
                       "resolution": "810*1440",
                       "dpi": "270",
                       "update_version_code": "73109",
                       "plugin": "0",
                       # "pos": "5r_-9Onkv6e_v7G_8fLz-vTp6Pn4v6esrKuzqa2tpK-osb_x_On06ej5-L-nrqSzpK2ur6Wosb_88Pzt3vTp5L-nv7-xv_zw_O3R8vP69Ono-fi_p6ysq7OpraqsqqiuraWprK2or7G__PD87dH86fTp6Pn4v6eupLOkramrpa2krKSrqq-sqajg",
                       # "fp": "w2TZFzTqczmWFlwOLSU1J2xecSKO",
                       "tma_jssdk_version": "1.24.0.1",
                       "rom_version": "coloros__v418ir release-keys"

                       }

            url = "https://ic.snssdk.com/api/feed/profile/v1/?%s" % urllib.parse.urlencode(url_dic)
            # print(url)
            get_page = requests.get(url, headers=headers, allow_redirects=False)
            # print(get_page.text)
            # time.sleep(0.5)
            # format_json = re.match(r"jsonp\d+", get_page.text)
            page_dic = {}
            try:
                page_dic = get_page.json()
                data_list = page_dic.get('data')
                has_more = page_dic.get('has_more')
                offset = str(page_dic.get("offset"))
            except:
                if data_list:
                    data_list = page_dic.get('data')
                    has_more = page_dic.get('has_more')
                else:
                    data_list = []
                    has_more = False
            # offset = page_dic.get('offset')

            if has_more is None:
                has_more = False
            if data_list == []:
                print("no data in releaser %s page %s" % (releaser_id, count))
                # print(page_dic)
                print(url)
                count += 1
                has_more = False
                continue
            else:
                count += 1
                for one_video in data_list:
                    # info_str = one_video.get('content')
                    info_dic = json.loads(one_video["content"])
                    video_dic = {}
                    video_dic['title'] = info_dic.get('title')
                    video_dic['url'] = info_dic.get('share_url')
                    video_dic['releaser'] = info_dic.get('source')
                    video_dic['releaserUrl'] = releaserUrl
                    release_time = info_dic.get('publish_time')
                    video_dic['release_time'] = int(release_time * 1e3)
                    video_dic['duration'] = info_dic.get('video_duration')
                    video_dic['play_count'] = info_dic.get('video_detail_info').get("video_watch_count")
                    video_dic['repost_count'] = info_dic.get('forward_info').get('forward_count')
                    video_dic['comment_count'] = info_dic.get('comment_count')
                    video_dic['favorite_count'] = info_dic.get('digg_count')
                    video_dic['video_id'] = info_dic.get('item_id')
                    video_dic['fetch_time'] = int(time.time() * 1e3)
                    video_dic['releaser_id_str'] = "toutiao_%s" % releaser_id
                    yield video_dic


    def time_range_video_num(self, start_time, end_time, url_list):
        data_lis = []
        info_lis = []
        columns = ["title","url","release_time","releaserUrl","duration"]
        dur_count = 0
        count_false = 0
        for dic in url_list:
            for res in self.releaser_page(dic["url"]):
                title = res["title"]
                link = res["url"]
                video_time = res["release_time"]
                video_time_str = datetime.datetime.fromtimestamp(video_time / 1000).strftime("%Y-%m-%d %H-%M-%S")
                print(res)

                if video_time:
                    if start_time < video_time:
                        if video_time < end_time:
                            data_lis.append((title, link, video_time_str, dic["url"],res["duration"]))
                            if int(res["duration"]) <= 600:
                                dur_count += 1
                    else:
                        count_false += 1
                        if count_false>30:
                            break
                        else:
                            continue
            csv_save = pd.DataFrame(data_lis)
            if data_lis:
                try:
                    csv_save.to_csv("%s.csv" % (dic["platform"] + "_" + dic["releaser"]), encoding="gb18030",
                                    header=columns)
                except:
                    csv_save.to_csv("%s.csv" % (dic["platform"] + "_" + dic["releaser"]), encoding="utf-8",
                                    header=columns)
            info_lis.append([dic["platform"], dic["releaser"], len(data_lis),dur_count])
            data_lis = []
        csv_save = pd.DataFrame(info_lis)
        csv_save.to_csv("%s.csv" % (datetime.datetime.now().strftime("%Y-%m-%d")), encoding="gb18030", mode='a',
                        header=None, index=None)


if __name__ == "__main__":
    test = Craler_toutiao()
    url_lis = [
        {"platform": "toutiao",
         "url": "https://www.toutiao.com/c/user/101821018151/#mid=1606406044690440",
         "releaser": "海豚聚乐部"
         },
    ]
    start_time = datetime.datetime(year=2019, month=6, day=6)
    end = datetime.datetime.now()
    start_time = 1556640000000
    end = 1559318400000
    test.time_range_video_num(start_time, end, url_lis)
