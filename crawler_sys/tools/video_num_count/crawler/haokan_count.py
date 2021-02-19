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
import random
from urllib import parse
from urllib.parse import urlencode


try:
    from crawler_sys.site_crawler.func_get_releaser_id import *
except:
    from crawler.func_get_releaser_id import *
from crawler.trans_strtime_to_timestamp import trans_strtime_to_timestamp
from crawler.trans_duration_str_to_second import trans_duration

class Craler_haokan(object):
    def get_releaser_id(self, releaserUrl):
        return get_releaser_id(platform="haokan", releaserUrl=releaserUrl)

    def video_page(self, url, vid=None):
        """
        For Haokan App, video_page method ONLY accept pass in vid, rather than
        video url.
        """
        if vid is None:
            return None

        # post_url = ('https://sv.baidu.com/haokan/api?'
        #             'cmd=comment/getreply&log=vhk&tn=1001128v&ctn=1001128v'
        #             '&imei=279014587228348'
        #             '&cuid=C577C0F8F6AA9FFE3E41CB0B3E507A14|843822785410972'
        #             '&os=android&osbranch=a0&ua=810_1440_270&ut=ALP-AL00_6.0.1_23_HUAWEI'
        #             '&apiv=4.6.0.0&appv=409011&version=4.9.1.10'
        #             '&life=1546591253&clife=1546591253&hid=B3697DD2F02F9A031714A93CCDF0A4C7'
        #             '&imsi=0&network=1'
        #             '&sids=1373_1-1436_4-1629_2-1647_1-1646_2-1708_1-1715_2'
        #             '-1736_1-1738_3-1739_1-1748_3-1754_2-1757_1-1767_1'
        #             '-1772_2-1776_1-1778_1-1780_3-1782_1-1786_2-1803_1'
        #             '-1805_2-1806_3-1814_2 HTTP/1.1')
        random_str = ''.join(random.sample(['z','y','x','w','v','u','t','s','r','q','p','o','n','m','l','k','j','i','h','g','f','e','d','c','b','a'], 4)).upper()
        post_url = 'https://sv.baidu.com/haokan/api?tn=1008350o&ctn=1008350o&os=ios&cuid=E8015FD33EC4EBA7B853AF10A50A02D705F02DECEFMBGNN{0}&osbranch=i0&ua=640_1136_326&ut=iPhone5%2C4_10.3.3&net_type=-1&apiv=5.1.0.10&appv=1&version=5.1.0.10&life=1563337077&clife=1563337077&sids=&idfa=E3FC9054-384B-485F-9B4C-936F33D7D099&hid=9F5E84EAEEE51F4C190ACE7AABEB915F&young_mode=0&log=vhk&location=&cmd=video/detail'.format(random_str)
        # raw header str:
        # header_str = ('Charset: UTF-8'
        #             'User-Agent: Mozilla/5.0 (Linux; Android 6.0.1; ALP-AL00 Build/V417IR; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/52.0.2743.100 Mobile Safari/537.36 haokan/4.9.1.10 (Baidu; P1 6.0.1)/IEWAUH_32_1.0.6_00LA-PLA/1001128v/C577C0F8F6AA9FFE3E41CB0B3E507A14%7C843822785410972/1/4.9.1.10/409011/1'
        #             'XRAY-TRACEID: 9624a81f-15e0-486e-b79f-d97b30c5b7d0'
        #             'XRAY-REQ-FUNC-ST-DNS: okHttp;1546598993248;0'
        #             'Content-Type: application/x-www-form-urlencoded'
        #             'Content-Length: 267'
        #             'Host: sv.baidu.com'
        #             'Connection: Keep-Alive'
        #             'Accept-Encoding: gzip'
        #             'Cookie: BAIDUID=1EA157CF3563181B98E5ABC1DED982D6:FG=1; BAIDUZID=805xaZQOUQRP3LqnkFs1bl2Bv-TD-CMHnotPgI4vkWabaQgbAx_tx4yMxTHzMBqpC0hwc6ZRa4xUFEkFwB3jxCO_Lg8d5s9gk9OSOeIowQ2k; BAIDUCUID=luvyi0aLHf0RuSajY8S2ug8fvi0u82uugi2IigiS2i80Pv8hYavG8jafv8gqO28EA'
        #             )
        headers = {
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate",
                'Charset': 'UTF-8',
                "Accept-Language": "zh-Hans-CN;q=1",
                "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_3 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gecko) Mobile/14G60 haokan/5.1.0.10 (Baidu; P2 10.3.3)/3.3.01_4,5enohP/381d/E7919FD33EC4EBA7B853AF10A50A02D705F02DECEFMBGNNIETE/1 HTTP/1.1",
                # "XRAY-REQ-FUNC-ST-DNS": "okHttp;1562813246444;0",
                # "XRAY-TRACEID": "5bd68916-4696-4bb3-b3a3-57a0c6a15949",
                'Content-Type': 'application/x-www-form-urlencoded',
                # 'Content-Length': '267',
                'Host': 'sv.baidu.com',
                'Connection': 'Keep-Alive',
                "X-Bfe-Quic": "enable=1",
                "Cookie": "BAIDUCUID=luBHiY8JSig3iHiZ0iSLi0O_v80Gi2iqlav6u_aCS8g1aH8H_iS9a0ivWu0dtQODzbXmA; BAIDUID=F2385E8E821854CA8BE4E30920EED52F:FG=1"
        }

        # raw post data is like:
        # 'comment%2Fgetreply=method%3Dget%26url_key%3D13089959609189000356%26pn%3D1%26rn%3D10%26child_rn%3D2%26need_ainfo%3D0%26type%3D0%26vid%3D13089959609189000356&video%2Fdetail=method%3Dget%26url_key%3D13089959609189000356%26log_param_source%3D%26vid%3D13089959609189000356'
        # which can be decoded as urlencode rule as
        # post_str_decoded = ('comment/getreply=method=get'
        #                     '&url_key=13089959609189000356&pn=1'
        #                     '&rn=10'
        #                     '&child_rn=2'
        #                     '&need_ainfo=0'
        #                     '&type=0'
        #                     '&vid=13089959609189000356'
        #                     '&video/detail=method=get'
        #                     '&url_key=13089959609189000356'
        #                     '&log_param_source='
        #                     '&vid=13089959609189000356')
        # We cannot directly nest dict within dict for post data, or
        # the '{' and '}' will be treated as straight character rather than
        # dictionary boundary, which will lead to un-expected results.
        # The correct way to do this is two-set urlencode.
        comment_getreplyDict = {'method': 'get',
                                # 'url_key': '13089959609189000356&pn=1',
                                'url_key': '%s&pn=1' % vid,
                                'rn': '10',
                                'child_rn': '2',
                                'need_ainfo': '0',
                                'type': '0',
                                # 'vid': '13089959609189000356',
                                'vid': vid,
                                }
        comment_getreplyEncodedStr = urlencode(comment_getreplyDict)
        video_detailDict = {'method': 'get',
                            # 'url_key': '13089959609189000356',
                            'url_key': vid,
                            'log_param_source': '',
                            # 'vid': '13089959609189000356'
                            'vid': vid,
                            }
        video_detailEncodedStr = urlencode(video_detailDict)
        post_data = {'comment/getreply': comment_getreplyEncodedStr,
                     'video/detail': video_detailEncodedStr}

        get_page = requests.post(post_url, data=post_data, headers=headers)
        try:
            page_dict = get_page.json()
        except:
            return None
        video_dict = {}
        try:
            videoD = page_dict['video/detail']['data']
            commntD = page_dict['comment/getreply']['data']
        except:
            return None
        try:
            video_dict['comment_count'] = int(commntD['comment_count'])
            video_dict['favorite_count'] = videoD['like_num']
        except Exception:
            return None
        else:
            video_dict['duration'] = videoD['duration']
            fetch_time = int(time.time() * 1e3)
            video_dict['fetch_time'] = fetch_time
            video_dict['play_count'] = videoD['playcnt']
            video_dict['release_time'] = videoD['publishTime'] * 1e3
            video_dict['releaser'] = videoD['author']
            video_dict['title'] = videoD['title']
            video_dict['video_id'] = vid
            partial_url = '{"nid":"sv_%s"}' % vid
            partial_url_encode = urllib.parse.quote_plus(partial_url)
            video_dict['url'] = ('https://sv.baidu.com/videoui/page/videoland?context=%s'
                                 % partial_url_encode)
            releaser_id = videoD['appid']
            video_dict['releaserUrl'] = 'https://haokan.baidu.com/haokan/wiseauthor?app_id=' + releaser_id
        return video_dict

    def get_releaser_follower_num(self, releaserUrl):
        releaser_id = self.get_releaser_id(releaserUrl)
        url = "https://sv.baidu.com/haokan/api?cmd=baijia/authorInfo&log=vhk&tn=1008621v&ctn=1008621v&imei=261721032526201&cuid=51BF00514520A03B32E6CA9D7443D8F8|504550857697800&bdboxcuid=&os=android&osbranch=a0&ua=810_1440_270&ut=MI%20NOTE%203_6.0.1_23_Xiaomi&apiv=4.6.0.0&appv=414011&version=4.14.1.10&life=1555296294&clife=1558350548&hid=02112F128209DD6BAF39CA37DE9C05E6&imsi=0&network=1&location={%22prov%22:%22%22,%22city%22:%22%22,%22county%22:%22%22,%22street%22:%22%22,%22latitude%22:39.911017,%22longitude%22:116.413562}&sids=1957_2-2193_3-2230_4-2320_1-2326_2-2353_1-2359_3-2376_1-2391_1-2433_4-2436_5-2438_1-2442_1-2443_2-2452_1-2457_2-2470_1-2480_2-2511_1-2525_4-2529_1-2537_1-2538_1-2540_1-2555_2-2563_1-2565_2-2568_1-2574_1-2575_1-2577_1-2582_1"
        headers = {
            "Host": "sv.baidu.com",
            "Connection": "keep-alive",
            "Content-Length": "60",
            "Charset": "UTF-8",
            "User-Agent": 'Mozilla/5.0 (Linux; Android 6.0.1; MI NOTE 3 Build/V417IR; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/52.0.2743.100 Mobile Safari/537.36 haokan/4.14.1.10 (Baidu; P1 6.0.1)/imoaiX_32_1.0.6_3+ETON+IM/1008621v/51BF00514520A03B32E6CA9D7443D8F8%7C504550857697800/1/4.14.1.10/414011/1',
            "X-Bfe-Quic": "enable=1",
            "XRAY-REQ-FUNC-ST-DNS": "okHttp;1558350575755;0",
            "XRAY-TRACEID": "be54291d-c13a-4a88-8337-9e70ad75d7d8",
            # "Cookie": "BAIDUID=13CD01ABA9F3F112EEE8880716798F35:FG=1; BAIDUZID=T0WvNv-W1ew-E8YpKuV10jpN5j2DYGOE4bUCyiIsT3Iun8dpVe7GQpFr7mFjzYFHEFOhQgdC_ixxf48KpG1iwQb7HiU9ypKA2obES0JACE_E; BAIDUCUID=gaHRu_u_v8gga2830u2uu_uCHilEi-uk_av9i0PDHtifa28fga26fgayvf_NP2ijA",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept-Encoding": "gzip, deflate"
        }
        post_dic = {"baijia/authorInfo": "method=get&app_id=%s" % releaser_id}
        get_page = requests.post(url, data=post_dic, headers=headers)
        res = get_page.json()
        try:
            follower_num = res.get("baijia/authorInfo").get("data").get("fansCnt")
            print('%s follower number is %s' % (releaserUrl, follower_num))
            return follower_num
        except:
            print("can't can followers")
    def web_first_pag(self,page_text):
        res = re.findall("window.__PRELOADED_STATE__ = {(.*)};",page_text, flags=re.DOTALL)
        #print(res)
        res = json.loads("{%s}"%res[0])
        apiData = {"apiData":{"video":res["video"]}}
        fans = res["author"]["fansCnt"]
        return apiData,fans

    def releaser_page_web(self, releaserUrl,
                          output_to_file=False,
                          filepath=None,
                          releaser_page_num_max=10000,
                          output_to_es_raw=False,
                          output_to_es_register=False,
                          push_to_redis=False,
                          es_index=None,
                          doc_type=None,
                          fetchFavoriteCommnt=True):
        pid = os.getpid()
        releaser_id = self.get_releaser_id(releaserUrl)
        print('releaser_id is %s' % releaser_id)
        result_lst = []
        # video_info = self.video_data
        page_num = 0
        has_more = True
        ctime = ""
        count_false = 0
        while page_num <= releaser_page_num_max and has_more:


            post_url = 'https://haokan.baidu.com/haokan/wiseauthor?app_id=1564003728536358&_api=1&_skip={0}&ctime={1}&_limit=10&video_type=media&sort_type=sort_by_time'.format(
                    page_num, ctime)
            headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
                    "referer": "https://haokan.baidu.com/haokan/wiseauthor?app_id=1564003728536358",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                    "accept": "*/*",
                    "accept-encoding": "gzip, deflate, br",
                    "accept-language": "zh,zh-CN;q=0.9",
                    "content-type": "application/x-www-form-urlencoded"
            }
            try:
                if page_num == 0:
                    get_page = requests.get(releaserUrl, headers=headers, timeout=3)
                    #print(get_page.text)
                    page_dic,fans_num = self.web_first_pag(get_page.text)
                    page_num += 1
                else:
                    get_page = requests.get(post_url, headers=headers, timeout=3)
                    page_dic = get_page.json()
                    page_num += 1
                    #print(page_dic)
            except:
                continue
            try:
                info_lst = page_dic['apiData']['video']['results']
            except:
                info_lst = []
            try:
                ctime = page_dic['apiData']['video']['ctime']
                has_more = page_dic['apiData']['video']['has_more']
                # if not has_more:
                #     has_more = False
            except:
                has_more = False
            if info_lst != []:
                count_false = 0
                print("Process %s is processing %s at page %s" % (pid, releaser_id, page_num))
                time.sleep(int(random.uniform(1, 2)))
                for line in info_lst:
                    video_data = {}
                    video_data['title'] = line['content']['title']
                    video_id = line['content']['vid']
                    video_data['video_id'] = video_id
                    # partial_url = '{"nid":"sv_%s"}' % video_id
                    # partial_url_encode = urllib.parse.quote_plus(partial_url)
                    video_data['url'] = line['content']["video_short_url"]
                    video_data['play_count'] = line['content']['playcnt']
                    video_data['favorite_count'] = int(line['content']['praiseNum'])
                    try:
                        video_data['comment_count'] = int(line['content']['commentNum'])
                    except:
                        video_data['comment_count'] = 0
                    video_data['releaser_followers_count'] = int(fans_num)
                    # print('like num is %s' % video_data['favorite_count'])
                    try:
                        video_data['duration'] = trans_duration(line['content']['duration'])
                    except:
                        video_data['duration'] = 0
                    video_data['releaser'] = line['content']['author']
                    video_data['releaser_id_str'] = "haokan_%s" % (releaser_id)
                    video_data['releaserUrl'] = 'https://haokan.baidu.com/haokan/wiseauthor?app_id=' + releaser_id
                    fetch_time = int(time.time() * 1e3)
                    video_data['fetch_time'] = fetch_time
                    releaser_time_str = line['content']['publish_time']
                    video_data['release_time'] = trans_strtime_to_timestamp(input_time=releaser_time_str)



                    print(video_id, releaser_time_str,
                          datetime.datetime.fromtimestamp(video_data['release_time'] / 1000), page_num)
                    #result_lst.append(video_data)
                    yield video_data
            else:
                count_false += 1
                if count_false <= 5:
                    continue
                else:
                    has_more = False
    # @logged
    def releaser_page(self, releaserUrl,
                      output_to_file=False,
                      filepath=None,
                      releaser_page_num_max=10000,
                      output_to_es_raw=False,
                      output_to_es_register=False,
                      push_to_redis=False,
                      es_index=None,
                      doc_type=None,
                      fetchFavoriteCommnt=True):
        """
        post_url never change, what matters is the post_dic
        """
        pid = os.getpid()
        releaser_id = self.get_releaser_id(releaserUrl)
        print('releaser_id is %s' % releaser_id)
        result_lst = []
        # video_info = self.video_data
        page_num = 1
        has_more = True
        post_url = 'https://sv.baidu.com/haokan/api?tn=1008350o&ctn=1008350o&imei=&cuid=E8123FD33EC4EBA7B853AF10A50A02D705F02DECEFMBGNNIETE&os=ios&osbranch=i0&ua=640_1136_326&ut=iPhone5%2C4_10.3.3&net_type=-1&apiv=5.1.0.10&appv=1&version=5.1.0.10&life=1563337077&clife=1563337077&sids=&idfa=E3FC9054-384B-485F-9B4C-936F33D7D099&hid=9F5E84EAEEE51F4C190ACE7AABEB915F&young_mode=0&log=vhk&location=&cmd=baijia/listall'
        retry = 0
        while page_num <= releaser_page_num_max and has_more:

            post_str = ('method=get&app_id=' + releaser_id + '&_skip='
                        + str(page_num) + '&_limit=20&_timg_cover=100,150,1000'
                                          '&video_type=media&sort_type=sort_by_time')
            post_dic = {'baijia/listall': post_str}

            headers = {'Charset': 'UTF-8',
                       'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 12_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 haokan/4.15.0.11 (Baidu; P2 12.4)/4.21_11,6daP/381d/AD02A18F4DD4F3A3DEF0E98A148C4E04866875307FMSDEIBSPO/1',
                       'Content-Type': 'application/x-www-form-urlencoded',
                       'Host': 'sv.baidu.com',
                       'Connection': 'Keep-Alive',
                       'Accept-Encoding': 'gzip, deflate',
                       # 'Cookie': 'BAIDUID=0FD29037A7BFCC0051D2EA95593A7EE1:FG=1; BAIDUZID=lOzvTPOaIXEe7OsZu-O1wAWwzNE5fY9RZotsVv2pvjPHBJOaJk3paZ48CZzB1ioQV0sFX9Hu4C7K_oHkm8HE9QL2XyEzf9cCzUrIjudbuSE8; BAIDUCUID=gi238laR280F82ie_avJ8g81valvaSa3luHx8YuAB88qa-8CgOvlfgaBvt_NP2ijA',
                       }
            try:
                get_page = requests.post(post_url, data=post_dic, headers=headers, timeout=3)
                page_dic = get_page.json()
            except:
                continue
            try:
                info_lst = page_dic['baijia/listall']['data']['results']
            except:
                info_lst = []
            if info_lst != []:
                print("Process %s is processing %s at page %s" % (pid, releaser_id, page_num))
                retry = 0
                page_num += 1
                time.sleep(int(random.uniform(1, 2)))
                for k, line in enumerate(info_lst):
                    # video_data = copy.deepcopy(self.video_data_template)
                    video_data = {}
                    video_data['title'] = line['content']['title']
                    video_id = line['content']['vid']
                    video_data['video_id'] = video_id
                    partial_url = '{"nid":"sv_%s"}' % video_id

                    partial_url_encode = urllib.parse.quote_plus(partial_url)
                    video_data['url'] = ('https://sv.baidu.com/videoui/page/videoland?context=%s'
                                         % partial_url_encode)
                    video_data['play_count'] = line['content']['playcnt']
                    video_data['favorite_count'] = line['content']['like_num']
                    # print('like num is %s' % video_data['favorite_count'])
                    try:
                        video_data['duration'] = line['content']['duration']
                    except:
                        video_data['duration'] = 0
                    video_data['releaser'] = line['content']['author']
                    # video_data['releaser_id_str'] = "haokan_%s" % (releaser_id)
                    video_data['releaserUrl'] = 'https://haokan.baidu.com/haokan/wiseauthor?app_id=' + releaser_id
                    fetch_time = int(time.time() * 1e3)
                    video_data['fetch_time'] = fetch_time
                    try:
                        releaser_time_str = line['content']['publish_time']
                    except:
                        pass
                    if fetchFavoriteCommnt:
                        for retry in range(4):
                            newVideoDict = self.video_page('', vid=video_id)
                            if newVideoDict:
                                break
                        if newVideoDict:
                            video_data['favorite_count'] = newVideoDict['favorite_count']
                            video_data['comment_count'] = newVideoDict['comment_count']
                            video_data['release_time'] = int(newVideoDict['release_time'])
                            # print('like num after video_page fetching is %s' % video_data['favorite_count'])
                            print(video_id, releaser_time_str,
                                  datetime.datetime.fromtimestamp(video_data['release_time'] / 1000), page_num)
                            # result_lst.append(video_data)
                            yield video_data
                        else:
                            continue
                    # video_data['release_time'] = int(newVideoDict['release_time'])
                    # yield video_data
            else:
                print("process %s can't get releaser %s at page %s" % (pid, releaser_id, page_num))
                page_num += 1
                time.sleep(int(random.uniform(1, 2)))
                retry += 1
                if retry <=5:
                    continue
                else:
                    has_more = False


    @staticmethod
    def video_time(time_str):
        now = datetime.datetime.now()
        if "分钟前" in time_str:
            min_str = re.findall(r"(\d+)分钟前", time_str)[0]
            videotime = now - datetime.timedelta(minutes=int(min_str))
        elif "小时前" in time_str:
            hour_str = re.findall(r"(\d+)小时前", time_str)[0]
            videotime = now - datetime.timedelta(hours=int(hour_str))
        elif "昨天" in time_str:
            date_lis = time_str.split(" ")
            hours, mins = date_lis[1].split(":")
            last_day = now - datetime.timedelta(days=1)
            videotime = datetime.datetime(year=int(last_day.year), month=int(last_day.month), day=int(last_day.day),
                                          hour=int(hours), minute=int(mins))
        elif "前天" in time_str:
            date_lis = time_str.split(" ")
            hours, mins = date_lis[1].split(":")
            last_day = now - datetime.timedelta(days=2)
            videotime = datetime.datetime(year=int(last_day.year), month=int(last_day.month), day=int(last_day.day),
                                          hour=int(hours), minute=int(mins))
        elif "天前" in time_str:
            day_str = re.findall(r"(\d+)天前", time_str)[0]
            videotime = now - datetime.timedelta(days=int(day_str))
        elif "刚刚" in time_str:
            videotime = now
        else:
            if str(now.year) in time_str:
                pass
            else:
                date_lis = time_str.split(" ")
                month, days = date_lis[0].split("-")
                hours, mins = date_lis[1].split(":")
                videotime = datetime.datetime(year=int(now.year), month=int(month), day=int(days), hour=int(hours),
                                              minute=int(mins))

        # print(videotime.strftime("%Y-%m-%d %H:%M:%S"))
        return videotime

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
                # print(res)

                if video_time:
                    if start_time < video_time:
                        if video_time < end_time:
                            data_lis.append((title, link, video_time_str, dic["url"],res["duration"]))
                            if int(res["duration"]) <= 600:
                                dur_count += 1
                    else:
                        count_false += 1
                        if count_false > 200:
                            break
                        else:
                            continue
            csv_save = pd.DataFrame(data_lis)
            if data_lis:
                try:
                    csv_save.to_csv("%s.csv" % (dic["platform"] + "_" + dic["releaser"]), encoding="gb18030",header=columns)
                except:
                    csv_save.to_csv("%s.csv" % (dic["platform"] + "_" + dic["releaser"]), encoding="utf-8",
                                    header=columns)
            info_lis.append([dic["platform"], dic["releaser"], len(data_lis),dur_count])
            data_lis = []

        csv_save = pd.DataFrame(info_lis)
        csv_save.to_csv("%s.csv" % (datetime.datetime.now().strftime("%Y-%m-%d")), encoding="gb18030", mode='a',
                        header=None, index=None)


if __name__ == "__main__":
    test = Craler_haokan()
    url_lis = [
        {"platform": "haokan",
         "url": "https://haokan.baidu.com/haokan/wiseauthor?app_id=1607668704882557",
         "releaser": "青春旅社"
         },
    ]
    start_time = datetime.datetime(year=2019, month=6, day=6)
    end = datetime.datetime.now()
    start_time = 1561219200000
    end = 1561824000000
    test.time_range_video_num(start_time, end, url_lis)
