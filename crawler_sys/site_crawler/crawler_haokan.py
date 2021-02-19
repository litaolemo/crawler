# -*- coding: utf-8 -*-
"""
Created on Wed Sep 26 21:04:19 2018

@author: fangyucheng
"""

import os
import re
import random
import copy
import json
import time, datetime
import requests
import urllib
import random
from urllib import parse
from urllib.parse import urlencode
from crawler_sys.utils.output_results import output_result
from crawler_sys.framework.video_fields_std import Std_fields_video
from crawler_sys.utils.trans_strtime_to_timestamp import trans_strtime_to_timestamp
from crawler_sys.utils.trans_duration_str_to_second import trans_duration
from crawler.crawler_sys.utils.util_logging import logged
from crawler.crawler_sys.site_crawler.crawler_baijiahao import *

try:
    from crawler_sys.framework.func_get_releaser_id import *
except:
    from func_get_releaser_id import *
from crawler.crawler_sys.utils.trans_duration_str_to_second import *
from crawler.crawler_sys.proxy_pool.func_get_proxy_form_kuaidaili import get_proxy


class Crawler_haokan():

    def __init__(self, platform='haokan'):
        self.platfrom = platform
        self.video_data_template = Std_fields_video().video_data
        self.video_data_template['platform'] = platform
        self.count_false = 0
        self.baijiahao = Crawler_baijiahao()
        pop_lst = ['channel', 'describe', 'isOriginal', 'repost_count']
        for key in pop_lst:
            self.video_data_template.pop(key)

    def baijiahao_relaser_page_web(self, releaserUrl,
                                   output_to_file=False,
                                   filepath=None,
                                   releaser_page_num_max=30,
                                   output_to_es_raw=False,
                                   output_to_es_register=False,
                                   push_to_redis=False,
                                   es_index=None,
                                   doc_type=None,
                                   fetchFavoriteCommnt=True):
        pass

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
        post_url = 'https://sv.baidu.com/haokan/api?tn=1008350o&ctn=1008350o&os=ios&cuid=E8019FD33EC4EBA7B853AF10A50A02D705F02DECEFMBGNNIETE&osbranch=i0&ua=640_1136_326&ut=iPhone5%2C4_10.3.3&net_type=-1&apiv=5.1.0.10&appv=1&version=5.1.0.10&life=1563337077&clife=1563337077&sids=&idfa=E3FC9054-384B-485F-9B4C-936F33D7D099&hid=9F5E84EAEEE51F4C190ACE7AABEB915F&young_mode=0&log=vhk&location=&cmd=video/detail'
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
                            'log_param_source': 'author',
                            # 'vid': '13089959609189000356'
                            'vid': vid,
                            # "adparam": r"""{"screen_type":0,"pid":"","ac":"1","install_timestamp":"","ext":"[{\"k\":\"video_vid\",\"v\":\"%s\"},{\"k\":\"iad\",\"v\":\"327681\"}]","ver":"5.1.0.10","mod":"VIVO X20","ov":"6.0.1","baiduId":"A6DC59055E4FC518778A19436C23B49A:FG=1","fmt":"json","apna":"com.baidu.haokan","eid":"1957_2,2193_3,2230_4,2320_1,2326_2,2353_1,2359_3,2376_1,2391_1,2433_4,2436_5,2438_1,2442_1,2443_2,2452_1,2457_2,2470_1,2480_2,2511_1,2525_4,2529_1,2537_1,2538_1,2540_1,2555_2,2563_1,2565_2,2568_1,2574_1,2575_1,2577_1,2582_1","ot":"2","ct":"2","nt":"1","android_id":"7313ae71df9e5367","iad":327681,"ua":"810_1399_android_5.1.0.10_270","apinfo":"na_z_vOm8vLw8fDy8v-sqvLys_by8fX_9__18ff28fbhpKiq6aWmrqOy6a-mqKymqeH1__H18_P1__X-8P728v_HA_..%7Cqloc2","latitude":"39.911017","longitude":"116.413562","source":"videolanding"}"""
                            }
        video_detailEncodedStr = urlencode(video_detailDict)
        post_data = {'comment/getreply': comment_getreplyEncodedStr,
                     'video/detail': video_detailEncodedStr}

        get_page = requests.post(post_url, data=post_data, headers=headers)
        # print(get_page.text)
        try:
            page_dict = get_page.json()
            print(page_dict)
        except:
            self.count_false += 1
            if self.count_false <= 5:
                self.video_page(url, vid=vid)
            else:
                return None
        self.count_false = 0
        video_dict = copy.deepcopy(self.video_data_template)
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
            try:
                video_dict['duration'] = videoD['duration']
                fetch_time = int(time.time() * 1e3)
                video_dict['fetch_time'] = fetch_time
                video_dict['play_count'] = videoD['playcnt']
                video_dict['release_time'] = int(videoD['publishTime'] * 1e3)
                video_dict['releaser'] = videoD['author']
                video_dict['title'] = videoD['title']
                video_dict['video_id'] = vid
                partial_url = '{"nid":"sv_%s"}' % vid
                partial_url_encode = urllib.parse.quote_plus(partial_url)
                video_dict['url'] = ('https://sv.baidu.com/videoui/page/videoland?context=%s'
                                     % partial_url_encode)
                releaser_id = videoD['appid']
                video_dict['releaserUrl'] = 'https://haokan.baidu.com/haokan/wiseauthor?app_id=' + releaser_id
                video_dict['releaser_id_str'] = 'haokan_' + releaser_id
                video_dict['video_img'] = videoD['cover_src']
            except:
                return None
        print(video_dict)
        return video_dict

    def get_releaser_id(self, releaserUrl):
        return get_releaser_id(platform=self.platfrom, releaserUrl=releaserUrl)

    def get_releaser_follower_num(self,releaserUrl):
        return self.baijiahao.get_releaser_follower_num(releaserUrl)

    def get_releaser_follower_num_web(self, releaserUrl):
        proxies = get_proxy(1)
        releaser_id = self.get_releaser_id(releaserUrl)
        url = "https://sv.baidu.com/haokan/api?cmd=baijia/authorInfo&log=vhk&tn=1008621v&ctn=1008621v&imei=&cuid=51BF00514510A03B32E6CA9D7443D8F8|504550857697800&bdboxcuid=&os=android&osbranch=a0&ua=810_1440_270&ut=MI%20NOTE%203_6.0.1_23_Xiaomi&apiv=4.6.0.0&appv=414011&version=4.14.1.10&life=1555296294&clife=1558350548&hid=02112F128209DD6BAF39CA37DE9C05E6&imsi=0&network=1&location={%22prov%22:%22%22,%22city%22:%22%22,%22county%22:%22%22,%22street%22:%22%22,%22latitude%22:39.911017,%22longitude%22:116.413562}&sids=1957_2-2193_3-2230_4-2320_1-2326_2-2353_1-2359_3-2376_1-2391_1-2433_4-2436_5-2438_1-2442_1-2443_2-2452_1-2457_2-2470_1-2480_2-2511_1-2525_4-2529_1-2537_1-2538_1-2540_1-2555_2-2563_1-2565_2-2568_1-2574_1-2575_1-2577_1-2582_1"
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
        get_page = requests.post(url, data=post_dic, headers=headers, proxies=proxies)
        res = get_page.json()
        try:
            follower_num = res.get("baijia/authorInfo").get("data").get("fansCnt")
            print('%s follower number is %s' % (releaserUrl, follower_num))
            return follower_num
        except:
            print("can't can followers")

    def web_first_pag(self, page_text):
        res = re.findall("window.__PRELOADED_STATE__ = {(.*)};", page_text, flags=re.DOTALL)
        # print(res)
        try:
            res = json.loads("{%s}" % res[0])
            apiData = {"apiData": {"video": res["video"]}}
            fans = res["author_info"]["fansCnt"]
            return apiData, fans
        except:
            return None, None

    def releaser_page_web(self, releaserUrl,
                          output_to_file=False,
                          filepath=None,
                          releaser_page_num_max=30,
                          output_to_es_raw=False,
                          output_to_es_register=False,
                          push_to_redis=False,
                          es_index=None,
                          doc_type=None,
                          fetchFavoriteCommnt=True,proxies_num=None):
        pid = os.getpid()
        releaser_id = self.get_releaser_id(releaserUrl)
        print('releaser_id is %s' % releaser_id)
        result_lst = []
        # video_info = self.video_data
        page_num = 0
        has_more = True
        ctime = ""
        count_false = 0
        # proxies = None
        proxies = get_proxy(proxies_num)
        while page_num <= releaser_page_num_max and has_more:

            post_url = 'https://haokan.baidu.com/haokan/wiseauthor?app_id={0}&_api=1&_skip={1}&ctime={2}&_limit=10&video_type=media&sort_type=sort_by_time'.format(
                    releaser_id, page_num, ctime)
            headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
                    "referer": "https://haokan.baidu.com/haokan/wiseauthor?app_id=1564003728536358",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                    "accept": "*/*",
                    "accept-encoding": "gzip, deflate",
                    "accept-language": "zh,zh-CN;q=0.9",
                    "content-type": "application/x-www-form-urlencoded"
            }
            try:
                if page_num == 0:
                    for loop in range(5):
                        get_page = requests.get(releaserUrl, headers=headers, timeout=3, proxies=proxies)
                        # print(get_page.text)
                        page_dic, fans_num = self.web_first_pag(get_page.text)
                        if page_dic['apiData']['video']['results']:
                            page_num += 1
                            break
                else:
                    get_page = requests.get(post_url, headers=headers, timeout=3)
                    page_dic = get_page.json()
                    page_num += 1
                    # print(page_dic)
            except:
                count_false += 1
                if count_false >= 5:
                    break
                continue
            try:
                info_lst = page_dic['apiData']['video']['results']
            except:
                info_lst = []
            try:
                ctime = page_dic['apiData']['video']['ctime']
                has_more = page_dic['apiData']['video']['has_more']
                if not has_more:
                    has_more = False
            except:
                has_more = False
            if info_lst != []:
                count_false = 0
                print("Process %s is processing %s at page %s" % (pid, releaser_id, page_num))
                time.sleep(int(random.uniform(1, 2)))
                for line in info_lst:
                    video_data = copy.deepcopy(self.video_data_template)
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
                    video_data['releaser_id_str'] = "haokan_%s" % (line['content']['authorid'])
                    video_data['releaserUrl'] = 'https://haokan.baidu.com/haokan/wiseauthor?app_id=' + line['content'][
                        'authorid']
                    fetch_time = int(time.time() * 1e3)
                    video_data['fetch_time'] = fetch_time
                    releaser_time_str = line['content']['publish_time']
                    video_data['release_time'] = trans_strtime_to_timestamp(input_time=releaser_time_str)
                    video_data['video_img'] = line['content']['cover_src']
                    print(video_id, releaser_time_str,
                          datetime.datetime.fromtimestamp(video_data['release_time'] / 1000), page_num)
                    result_lst.append(video_data)
                if len(result_lst) >= 100:
                    output_result(result_Lst=result_lst,
                                  platform=self.platfrom,
                                  output_to_file=output_to_file,
                                  filepath=filepath,
                                  push_to_redis=push_to_redis,
                                  output_to_es_register=output_to_es_register,
                                  output_to_es_raw=output_to_es_raw,
                                  es_index=es_index,
                                  doc_type=doc_type)
                    result_lst.clear()

        if result_lst != []:
            output_result(result_Lst=result_lst,
                          platform=self.platfrom,
                          output_to_file=output_to_file,
                          filepath=filepath,
                          push_to_redis=push_to_redis,
                          output_to_es_register=output_to_es_register,
                          output_to_es_raw=output_to_es_raw,
                          es_index=es_index,
                          doc_type=doc_type)


    def releaser_page_baijiahao(self, releaserUrl,
                      output_to_file=False,
                      filepath=None,
                      releaser_page_num_max=40,
                      output_to_es_raw=False,
                      output_to_es_register=False,
                      push_to_redis=False,
                      es_index=None,
                      doc_type=None,
                      fetchFavoriteCommnt=True,proxies_num=None):
        self.baijiahao.releaser_page(releaserUrl, output_to_file=output_to_file,
                               filepath=filepath,
                               releaser_page_num_max=releaser_page_num_max,
                               output_to_es_raw=output_to_es_raw,
                               output_to_es_register=output_to_es_register,
                               push_to_redis=push_to_redis,
                               es_index=es_index,
                               doc_type=doc_type,
                               fetchFavoriteCommnt=fetchFavoriteCommnt,proxies_num=proxies_num)
        self.baijiahao.releaser_dynamic_page_web(releaserUrl, output_to_file=output_to_file,
                               filepath=filepath,
                               releaser_page_num_max=releaser_page_num_max,
                               output_to_es_raw=output_to_es_raw,
                               output_to_es_register=output_to_es_register,
                               push_to_redis=push_to_redis,
                               es_index=es_index,
                               doc_type=doc_type,
                               fetchFavoriteCommnt=fetchFavoriteCommnt,proxies_num=proxies_num)
    # @logged
    def releaser_page(self, releaserUrl,
                      output_to_file=False,
                      filepath=None,
                      releaser_page_num_max=40,
                      output_to_es_raw=False,
                      output_to_es_register=False,
                      push_to_redis=False,
                      es_index=None,
                      doc_type=None,
                      fetchFavoriteCommnt=True,
                      proxies_num=None
                      ):
        self.releaser_page_web(releaserUrl, output_to_es_raw=output_to_es_raw, es_index=es_index,
                                              doc_type=doc_type,
                                              releaser_page_num_max=50,proxies_num=proxies_num)
        self.releaser_page_baijiahao(releaserUrl, output_to_file=output_to_file,
                               filepath=filepath,
                               releaser_page_num_max=releaser_page_num_max,
                               output_to_es_raw=output_to_es_raw,
                               output_to_es_register=output_to_es_register,
                               push_to_redis=push_to_redis,
                               es_index=es_index,
                               doc_type=doc_type,
                               fetchFavoriteCommnt=fetchFavoriteCommnt,
                                     proxies_num=proxies_num)

    def releaser_page_app(self, releaserUrl,
                          output_to_file=False,
                          filepath=None,
                          releaser_page_num_max=40,
                          output_to_es_raw=False,
                          output_to_es_register=False,
                          push_to_redis=False,
                          es_index=None,
                          doc_type=None,
                          fetchFavoriteCommnt=True,proxies_num=None):

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
        post_url = 'https://sv.baidu.com/haokan/api?tn=1008350o&ctn=1008350o&imei=&cuid=E7142FD33EC4EBA7B853AF10A50A02D{0}02DECEFMBGNNICXT&os=ios&osbranch=i0&ua=640_1136_326&ut=iPhone5%2C4_10.3.3&net_type=-1&apiv=5.1.0.10&appv=1&version=5.1.0.10&life=1563337077&clife=1563337077&sids=&idfa=E3FC9054-384B-485F-9B4C-936F33D7D099&hid=9F5E84EAEEE51F4C190ACE7AABEB915F&young_mode=0&log=vhk&location=&cmd=baijia/listall'.format(
                random.randint(1000, 9999))

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
            # proxies = {
            #        "HTTPS":"http://hanye:i9mmu0a@180.103.1.225:17796"
            # }
            # post_dic = r"baijia%2Flistall=method%3Dget%26app_id%3D1565285080839434%26_skip%3D1%26_limit%3D20%26_timg_cover%3D100%2C150%2C1000%26video_type%3Dmedia%26sort_type%3Dsort_by_time"
            try:
                # print(post_dic)
                get_page = requests.post(post_url, data=post_dic, headers=headers, timeout=3)
                print(dict(get_page.cookies))
                page_dic = get_page.json()
            except:
                continue
            try:
                info_lst = page_dic['baijia/listall']['data']['results']
            except:
                print(page_dic)
                info_lst = []
            if info_lst != []:
                print("Process %s is processing %s at page %s" % (pid, releaser_id, page_num))
                page_num += 1
                time.sleep(int(random.uniform(1, 2)))
                for line in info_lst:
                    video_data = copy.deepcopy(self.video_data_template)
                    video_data['title'] = line['content']['title']
                    video_id = line['content']['vid']
                    video_data['video_id'] = video_id
                    # partial_url = '{"nid":"sv_%s"}' % video_id
                    video_data['url'] = line['content']["video_short_url"]
                    video_data['play_count'] = line['content']['playcnt']
                    video_data['favorite_count'] = line['content']['like_num']
                    # print('like num is %s' % video_data['favorite_count'])
                    try:
                        video_data['duration'] = line['content']['duration']
                    except:
                        video_data['duration'] = 0
                    video_data['releaser'] = line['content']['author']
                    video_data['releaser_id_str'] = "haokan_%s" % (releaser_id)
                    video_data['releaserUrl'] = 'https://haokan.baidu.com/haokan/wiseauthor?app_id=' + releaser_id
                    fetch_time = int(time.time() * 1e3)
                    video_data['fetch_time'] = fetch_time
                    releaser_time_str = line['content']['publish_time']
                    # video_data['release_time'] = trans_strtime_to_timestamp(input_time=releaser_time_str)

                    if fetchFavoriteCommnt:
                        newVideoDict = self.video_page('', vid=video_id)
                        if newVideoDict:
                            video_data['favorite_count'] = newVideoDict['favorite_count']
                            video_data['comment_count'] = newVideoDict['comment_count']
                            video_data['release_time'] = int(newVideoDict['release_time'])
                            # print('like num after video_page fetching is %s' % video_data['favorite_count'])
                            print(video_id, releaser_time_str,
                                  datetime.datetime.fromtimestamp(video_data['release_time'] / 1000), page_num)
                            result_lst.append(video_data)
                    if len(result_lst) >= 100:
                        output_result(result_Lst=result_lst,
                                      platform=self.platfrom,
                                      output_to_file=output_to_file,
                                      filepath=filepath,
                                      push_to_redis=push_to_redis,
                                      output_to_es_register=output_to_es_register,
                                      output_to_es_raw=output_to_es_raw,
                                      es_index=es_index,
                                      doc_type=doc_type)
                        result_lst.clear()
            else:
                print("process %s can't get releaser %s at page %s" % (pid, releaser_id, page_num))
                page_num += 1
                time.sleep(int(random.uniform(1, 2)))
                has_more = False
                break
        if result_lst != []:
            output_result(result_Lst=result_lst,
                          platform=self.platfrom,
                          output_to_file=output_to_file,
                          filepath=filepath,
                          push_to_redis=push_to_redis,
                          output_to_es_register=output_to_es_register,
                          output_to_es_raw=output_to_es_raw,
                          es_index=es_index,
                          doc_type=doc_type)

    def releaser_id_to_uk(self, releaser_id):
        headers = {'Charset': 'UTF-8',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0',
                   'Content-Type': 'application/x-javascript; charset=utf-8',
                   'Host': 'author.baidu.com',
                   'Connection': 'Keep-Alive',
                   'Accept-Encoding': 'gzip',
                   'Cookie': 'BAIDUID=5B4BD931D455EA625D8B5E20BD348270:FG=1; BIDUPSID=5B4BD931D455EA625D8B5E20BD348270; PSTM=1540776027; BDORZ=FFFB88E999055A3F8A630C64834BD6D0; H_PS_PSSID=1423_27211_21123_28131_27750_28139_20718; BDSFRCVID=Y2PsJeCCxG37oNO9K0MmeTd-epk7qPMdDVTa3J; H_BDCLCKID_SF=tR333R7oKRu_HRjYbb__-P4DHUjHfRO2X5REVMTHBPOkeqOJ2Mt5jP4NXNriJnOCfgjtXxcc5q_MoCDzbpnp05tpeGLsaPoy2K6XsJoq2RbhKROvhjntK6uQ-nnjhjnWLbneaJ5n0-nnhI3vXxPByTODyfQwXpoO0KcG_UFhHR3rsftRy6CaePk_hURK2D6aKC5bL6rJabCQe4_ZK-brKbTM0tvrbMT-027OKK85ahrcbqkxXtvI5lRBKtOh3j3zt4jMMh5xthF0hDvd-tnO-t6H-xQ0KnLXKKOLVMI-LPOkeqOJ2Mt5jP4NXNriJUrL5GnbsR5M2K3aVh6gQhjx-jtpexbH55utfnID3J; delPer=0; PSINO=2'}
        p = {'context': str({"from": 0, "app_id": releaser_id}).replace('\'', '\"')}
        rq_get = requests.get(
                'https://author.baidu.com/profile?pagelets=root', headers=headers, params=p)
        # print(rq_get.url)
        # print(rq_get.text[24:-2])
        info = json.loads(rq_get.text[24:-2])
        spts_list = info['scripts']
        spts = ' '.join(spts_list)
        uk_list = re.findall(r"\"uk\":\"(.+?)\"", spts)
        uk = uk_list[0]
        return uk

    def releaser_page_via_m(self, releaserUrl,
                            output_to_file=False,
                            filepath=None,
                            releaser_page_num_max=30,
                            output_to_es_raw=False,
                            output_to_es_register=False,
                            push_to_redis=False,
                            es_index=None,
                            doc_type=None):
        releaser_id = self.get_releaser_id(releaserUrl)
        uk = self.releaser_id_to_uk(releaser_id)
        print("platform: %s releaser_id: %s uk: %s" % (self.platform, releaser_id, uk))
        result_lst = []
        video_info = self.video_data_template
        page_count = 1
        headers = {'Charset': 'UTF-8',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0',
                   'Content-Type': 'application/x-javascript; charset=utf-8',
                   'Host': 'author.baidu.com',
                   'Connection': 'Keep-Alive',
                   'Accept-Encoding': 'gzip',
                   'Cookie': 'BAIDUID=5B4BD931D455EA625D8B5E20BD348270:FG=1; BIDUPSID=5B4BD931D455EA625D8B5E20BD348270; PSTM=1540776027; BDORZ=FFFB88E999055A3F8A630C64834BD6D0; H_PS_PSSID=1423_27211_21123_28131_27750_28139_20718; BDSFRCVID=Y2PsJeCCxG37oNO9K0MmeTd-epk7qPMdDVTa3J; H_BDCLCKID_SF=tR333R7oKRu_HRjYbb__-P4DHUjHfRO2X5REVMTHBPOkeqOJ2Mt5jP4NXNriJnOCfgjtXxcc5q_MoCDzbpnp05tpeGLsaPoy2K6XsJoq2RbhKROvhjntK6uQ-nnjhjnWLbneaJ5n0-nnhI3vXxPByTODyfQwXpoO0KcG_UFhHR3rsftRy6CaePk_hURK2D6aKC5bL6rJabCQe4_ZK-brKbTM0tvrbMT-027OKK85ahrcbqkxXtvI5lRBKtOh3j3zt4jMMh5xthF0hDvd-tnO-t6H-xQ0KnLXKKOLVMI-LPOkeqOJ2Mt5jP4NXNriJUrL5GnbsR5M2K3aVh6gQhjx-jtpexbH55utfnID3J; delPer=0; PSINO=2'}

        params1 = {'type': 'video', 'tab': '9', 'uk': uk,
                   # 'ctime': '15448673604154',
                   # '_': '1545633915094',
                   'callback': 'jsonp5'}
        rq_get1 = requests.get('https://author.baidu.com/list', params=params1, headers=headers)
        page_info1 = json.loads(rq_get1.text[7: -2])
        releaser = page_info1['user']['display_name']

        def handle_one_video(one, video_info, releaser, releaserUrl, platform):
            video_data = copy.deepcopy(video_info)

            video_itemid = one['attr']['itemId']
            find_asyncData = one['asyncData']

            video_data['platform'] = platform
            video_data['releaser'] = releaser
            video_data['releaserUrl'] = releaserUrl
            video_data['title'] = one['title']
            video_data['url'] = r'https://sv.baidu.com/videoui/page/videoland?context=' + parse.quote(
                    '{"nid":"sv_%s"}' % \
                    one['id'][3:])
            video_data['duration'] = trans_duration(one['timeLong'])
            video_data['video_id'] = one['article_id']
            video_data['release_time'] = int(one['publish_at']) * 1000
            fetch_time = int(time.time() * 1e3)
            video_data['fetch_time'] = fetch_time

            params2 = {'params': json.dumps([find_asyncData]), 'uk': uk, '_': str(int(time.time()) * 1000)}
            rq_get2 = requests.get(
                    'https://mbd.baidu.com/webpage?type=homepage&action=interact&format=jsonp&callback=jsonp2',
                    params=params2)
            page_info2 = json.loads(rq_get2.text[7: -1])
            try:
                video_data['play_count'] = int(page_info2['data']['user_list'][video_itemid]['read_num'])
            except:
                video_data['play_count'] = 0
            try:
                video_data['favorite_count'] = int(page_info2['data']['user_list'][video_itemid]['praise_num'])
            except:
                video_data['favorite_count'] = 0
            try:
                video_data['comment_count'] = int(page_info2['data']['user_list'][video_itemid]['comment_num'])
            except:
                video_data['comment_count'] = 0
            return video_data

        while page_info1['data']['has_more'] == 1 and page_count < releaser_page_num_max:
            time.sleep(random.randint(4, 6))
            print("get data at page: %s" % str(page_count))
            ctime = page_info1['data']['ctime']
            for one in page_info1['data']['list']:
                one_result = handle_one_video(one, video_info, releaser, releaserUrl, self.platform)
                result_lst.append(one_result)
                if len(result_lst) >= 100:
                    output_result(result_Lst=result_lst,
                                  platform=self.platform,
                                  output_to_file=output_to_file,
                                  filepath=filepath,
                                  push_to_redis=push_to_redis,
                                  output_to_es_register=output_to_es_register,
                                  output_to_es_raw=output_to_es_raw,
                                  es_index=es_index,
                                  doc_type=doc_type)
                    result_lst.clear()
            params1['ctime'] = ctime
            rq_next_page = requests.get('https://author.baidu.com/list', params=params1, headers=headers)
            page_info1 = json.loads(rq_next_page.text[7:-2])
            page_count += 1
        for one in page_info1['data']['list']:
            one_result = handle_one_video(one, video_info, releaser, releaserUrl, self.platform)
            result_lst.append(one_result)
        output_result(result_Lst=result_lst,
                      platform=self.platform,
                      output_to_file=output_to_file,
                      filepath=filepath,
                      push_to_redis=push_to_redis,
                      output_to_es_register=output_to_es_register,
                      output_to_es_raw=output_to_es_raw,
                      es_index=es_index,
                      doc_type=doc_type)


if __name__ == '__main__':
    crawler = Crawler_haokan()
    releaserUrl = 'https://haokan.baidu.com/author/1643723115550456'
    # releaserUrl1 = 'https://haokan.hao123.com/haokan/wiseauthor?app_id=1593559247255945'
    # 看剧汪星人
    test = crawler.releaser_page_web(releaserUrl, output_to_es_raw=True, es_index='crawler-data-raw',
                                              doc_type='doc',
                                              releaser_page_num_max=50,proxies_num=1)
    # crawler.get_releaser_follower_num(releaserUrl)
    crawler.video_page("","7416383329700309282")
    # crawler.video_page("http://haokan.baidu.com/v?vid=10262055952255650719",vid ="10262055952255650719")
    #crawler.releaser_page()
