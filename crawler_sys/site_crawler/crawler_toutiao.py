# -*- coding: utf-8 -*-
"""
Created on Thu Mar 15 15:23:08 2018

@author: fangyucheng

Edited by hanye on 2018-05-15

Edited by fangyucheng on 2018-05-17

Edited by litao on 2019-04-10
"""

import copy
import datetime
import json
import random
import re
import time
import urllib

try:
    from crawler_sys.framework.func_get_releaser_id import *
except:
    from func_get_releaser_id import *
from crawler.crawler_sys.proxy_pool.func_get_proxy_form_kuaidaili import get_proxy
import requests

from crawler.crawler_sys.framework.get_redirect_resp import get_redirected_resp
from crawler.crawler_sys.utils.get_toutiao_as_cp_signature import as_cp
from crawler.crawler_sys.framework.video_fields_std import Std_fields_video
from crawler.crawler_sys.site_crawler.toutiao_get_signature import getHoney
from crawler.crawler_sys.utils.output_results import output_result
from crawler.crawler_sys.utils.output_results import retry_get_url
from crawler.crawler_sys.utils.trans_duration_str_to_second import trans_duration
# from crawler.crawler_sys.utils import output_log
from crawler.crawler_sys.utils.util_logging import logged


class Crawler_toutiao():

    def __init__(self, timeout=None, platform='toutiao'):
        if timeout is None:
            self.timeout = 10
        else:
            self.timeout = timeout
        self.platform = platform
        std_fields = Std_fields_video()
        self.video_data = std_fields.video_data
        self.video_data['platform'] = self.platform
        # remove fields that crawled data don't have
        pop_key_Lst = ['channel', 'describe', 'isOriginal', "repost_count", "video_id"]
        for popk in pop_key_Lst:
            self.video_data.pop(popk)
        self.releaser_url_pattern = 'http://www.365yg.com/c/user/[RELEASER_ID]/'

        self.list_page_url_dict = {'all_channel': (
                'https://www.365yg.com/api/pc/feed/?max_behot_time=0'
                '&category=video_new&utm_source=toutiao')}
        self.legal_list_page_urls = []
        self.legal_channels = []
        for ch in self.list_page_url_dict:
            list_page_url = self.list_page_url_dict[ch]
            self.legal_list_page_urls.append(list_page_url)
            self.legal_channels.append(ch)
        self.api_list = [
                "ic",
                "is",
                "api3-normal-c-hl",
                "ib",
                "api3-normal-c-lf",
                "id",
                "ie",
                "api3-normal-c-lq",
                "ii",
                "io",
                "it",
                "iu",
                "lf",
                "lg",
                "lh",
        ]
        #        log_path = '/home/hanye/crawlersNew/crawler/crawler_log'
        #        current_day = str(datetime.datetime.now())[:10]
        #        info_log_file = log_path + '/all_' + current_day + '.log'
        #        info_name = self.platform + '_info'
        #        error_log_file = log_path + '/error_' + current_day + '.log'
        #        error_name = self.platform + '_error'
        #        self.loggerinfo = output_log.init_logger(name=info_name, log_file=info_log_file)
        #        self.loggererror = output_log.init_logger(name=error_name, log_file=error_log_file)
        # self.headers = {
        #
        #     # "Host": "api3-normal-c-hl.snssdk.com",
        #     "Connection": "Keep-Alive",
        #     "accept-encoding": "gzip, deflate",
        #     "x-ss-sessionid": "",
        #     "sdk-version": "2",
        #     # "cookie": "ttreq=1$b72ff1a4bec1a13b4e42dc12a741db51c0b2d52b; odin_tt=6664b911161b73ecb325b187e53bb16a2d2702779d2d74ec168a8ac182b33eb6e273ee261d6f965442fc71e3efa2f581a39e3c4c58a8ea33ff87c7103d4833db",
        #     # "x-tt-trace-id": "00-eea570870a10554009a6731d771f000d-eea570870a105540-01",
        #     # "tt-request-time": "4294967295",
        #     # "x-ss-cookie": "odin_tt=6664b911161b73ecb325b187e53bb16a2d2702779d2d74ec168a8ac182b33eb6e273ee261d6f965442fc71e3efa2f581a39e3c4c58a8ea33ff87c7103d4833db; ttreq=1$b72ff1a4bec1a13b4e42dc12a741db51c0b2d52b",
        #     # "x-ss-dp": "13",
        #     # "passport-sdk-version": "5.1.0",
        #     # "user-agent": "News 7.5.4 rv:7.5.4.10 (iPhone; iOS 10.3.3; zh_CN) Cronet",
        #     # "X-Gorgon": "0401f049000019b9cb347d6a6db31a12f642c82f8052c7bd8ba0",
        #     # "X-Khronos": str(int(datetime.datetime.now().timestamp())),
        #     "sec-fetch-user": "?1",
        #     "cache-control": "?max-age=0",
        #     "sec-fetch-site": "none",
        #     "upgrade-insecure-requests": "1",
        #
        # }
        # self.headers = {
        #         "Connection": "keep-alive",
        #         # "User-Agent": "News 7.6.0 rv:7.6.0.14 (iPad; iOS 13.3.1; zh_CN) Cronet",
        #         "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 5.1.1; OPPO R11 Build/NMF26X) NewsArticle/7.1.5 cronet/TTNetVersion:2996a052 2019-08-12",
        #         "sdk-version": "2",
        #         # "x-ss-sessionid": "",
        #         # "passport-sdk-version": "5.4.0",
        #         # "X-SS-DP": "13",
        #         "Accept-Encoding": "gzip, deflate",
        #         # "X-Gorgon": "030000003005ed4d41d9d6e0dd5c9b0dbb917c%sc282d" % random.randint(401800000,401952122),
        #
        # }
        self.headers = {

                "accept": "text/javascript, text/html, application/xml, text/xml, */*",
                "accept-encoding": "gzip, deflate",
                "accept-language": "zh,zh-CN;q=0.9",
                "content-type": "application/x-www-form-urlencoded",
                # "cookie": "gftoken=MjA4NTcyMDkyMXwxNTgyOTYxNjM3NjZ8fDAGBgYGBgY; SLARDAR_WEB_ID=9706fc8c-b8a6-4265-8a2e-e3f0739daaf2; UM_distinctid=1708fddb4c0466-04c756d28410e1-752c6c3c-51abc-1708fddb4c1790; CNZZDATA1274386066=608234173-1582960977-https%253A%252F%252Fwww.toutiao.com%252F%7C1582960977",
                # "referer": "https://profile.zjurl.cn/rogue/ugc/profile/?user_id=50502346296&media_id=50502346296&request_source=1",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36",
                "x-requested-with": "XMLHttpRequest",

        }

    def extract_field(self, raw_str, raw_field_name):
        try:
            field_value = (re.findall('%s:.+?,' % raw_field_name, raw_str)[0]
                               .replace('%s:' % raw_field_name, '')[:-1])
            # remove string start space and single quotation marks
            field_value_cleaned = re.sub('\'$', '', re.sub('^\s+?\'', '', field_value))
        except:
            field_value_cleaned = None
        return field_value_cleaned

    def get_host_str(self, url):
        get_host_str = re.findall('://.+?/', url)
        if get_host_str != []:
            host_str = get_host_str[0].replace(':', '').replace('/', '')
        else:
            host_str = None
        return host_str

    def video_page_via_m(self, url):
        url_video_id_midstep = ' '.join(re.findall('com/.*', url)).replace('com/', '')
        url_video_id = re.findall('\d+', url_video_id_midstep)[0]
        #        mobile_url = 'https://m.365yg.com/i'+url_video_id+'/info/?'
        mobile_url = 'https://m.365yg.com/i' + url_video_id
        get_page = retry_get_url(mobile_url)
        if get_page is not None:
            return None
        else:
            page = get_page.text
            page = page.replace('true', 'True')
            page = page.replace('false', 'False')
            page = page.replace('null', '"Null"')
            try:
                page_dic = eval(page)
            except:
                page_dic = None
                print('Failed to transfer text to dict on url: %s' % url)
                return None
            video_dict = copy.deepcopy(self.video_data)
            try:
                video_dic = page_dic['data']
                title = video_dic['title']
                releaser = video_dic['source']
                releaser_id = video_dic['creator_uid']
                releaserUrl = self.releaser_url_pattern.replace('[RELEASER_ID]',
                                                                str(releaser_id))
                play_count = video_dic['video_play_count']
                comment_count = video_dic['comment_count']
                release_time = int(video_dic['publish_time'] * 1e3)
                video_id = video_dic['video_id']
                fetch_time = int(datetime.datetime.now().timestamp() * 1e3)
                #
                video_dict['title'] = title
                video_dict['url'] = url
                video_dict['play_count'] = play_count
                video_dict['comment_count'] = comment_count
                video_dict['video_id'] = video_id
                video_dict['releaser'] = releaser
                video_dict['releaser_id_str'] = str(releaser_id)
                video_dict['releaserUrl'] = releaserUrl
                video_dict['release_time'] = release_time
                video_dict['fetch_time'] = fetch_time
            except:
                print('Failed when extracting data from page of url: %s' % url)
        return video_dict

    def video_page(self, url):
        """
        release_time is missing, should update this field when inserting into es.
        url such as 'http://toutiao.com/group/6532819433331622404/' can not get data now
        suggestion by fangyucheng is to rebuild url as
        www.365yg.com with video_id to sovle the problem
        """

        if "item" in url or "group" in url:
            vid = re.findall("/(\d+)/", url)[0]
        elif "xigua" in url:
            vid = re.findall("/i(\d+)/", url)[0]
        else:
            print(url)
            return None
        headers = {
                "Accept-Encoding": "gzip",
                "X-SS-REQ-TICKET": str(int(datetime.datetime.now().timestamp()) * 1e3),
                "sdk-version": "1",
                # "Cookie": "qh[360]=1; install_id=85200129335; ttreq=1$e8e97b875965bf4af4b5dbaaba4d4a5ec3441e47; history=JM89SDpxGAfw5%2F%2Bo%2F7tEz15%2FZ0tbUEN7Q8FhEYQQIdJ2oNFBgpagCA7BIbUFNUT0NjSkRIvl2AveOdr2XEVUuDS0FFnQEETEo%2BOH5%2Fvj9%2F0WyqF4xphMZNLJeD6aSBmk15Tt4nTWSGUaEHR0e%2BG9aqGfPFOgOXrZ%2BtQBJVI6QXPA89R9dzs2QCqC6eil7H3eQhcFiJOXE4NLgDL9q7FscXLM78Qv62rk0GuiRN511vlNRZioEEArGesNaKhQXxBmHd1q7ic19JNcb90Cu1ELfdQz11KkY4Ob%2BWZYex%2BRPCfFK6uaO12GkJ%2FEN%2BtofMgAVEg8s0qbw2ehgkKiwToovMVNdJP4ai%2Fqvw4vjlLXFi%2BqefWmhTKpUvum%2FoR3VBIvYDrgeYT5YtpNksxJe6WeA3SReODW1diayV1cq%2FzDhf2%2FoqFMognaHwAAAP%2F%2F; odin_tt=8cd4f07f6dc385b01edd52312dd29fbe7fdbfa059194493779de3fe408b8836bb9265292bb9335bc976037dd93e5d131de7acf894a805930417b4d3be7f308e0",
                # "X-Gorgon": "0300ddd08400675de6e75ad03849011c863306ddae2b0eb3cec4",
                # "X-Khronos": str(int(datetime.datetime.now().timestamp())),
                # "Host": "xgapi.snssdk.com",
                "Connection": "Keep-Alive",
                "Authorization": "HMAC-SHA1:2.0:1573091168911407306:bab42eac5b9e4a8eb25a91fc371ad533:WTfDrhnIsymHfmHCgG9YvRSu2YY=",
                "User-Agent": "okhttp/3.10.0.1",
                "X-Pods": "",
        }

        print(vid)
        url_dic = {
                "group_id": vid,
                "item_id": vid,
                "aggr_type": 0,
                "context": 1,
                "flags": 64,
                # "iid": "77627602260",
                # "device_id": random.randint(50000000000,59999999999),
                "ac": "wifi",
                "channel": "update",
                "aid": "13",
                "app_name": "news_article",
                "version_code": "732",
                "version_name": "7.3.2",
                "device_platform": "android",
                "ab_version": "830855,947965,942635,662176,665176,674051,643894,919834,649427,677130,710077,801968,707372,661900,668775,990369,739390,662099,668774,765190,976875,857803,952277,757281,679101,660830,759657,661781,648315",
                "ab_group": "100168",
                "ab_feature": "94563,102749",
                "ssmix": "a",
                "device_type": "oppo R11s Plus",
                "device_brand": "OPPO",
                "language": "zh",
                "os_api": "23",
                "os_version": "6.0.1",
                # "uuid": "250129616283002",
                # "openudid": "7313ae71df9e5367",
                "manifest_version_code": "731",
                "resolution": "810*1440",
                "dpi": "270",
                "update_version_code": "75410",
                "_rticket": int(datetime.datetime.now().timestamp() * 1e3),
                # "rom_version": "coloros__v417ir release-keys",
                # "fp": "w2TZFzTqczmWFlwOLSU1J2xecSKO",
                # "tma_jssdk_version": "1.24.0.1",
                # "pos": "5r_x8vP69Ono-fi_p6ysq7Opra2kr6ixv_H86fTp6Pn4v6eupLOkra6vpajg",
                "plugin": "0",
                # "ts":int(datetime.datetime.now().timestamp()),
                #     "as":"ab7f9fce505d1d7dbe7f9f",
                #     "mas":"011993339399f959a359d379b98587814259a359d3997919d319b3"
        }
        url = 'http://xgapi.snssdk.com/video/app/article/information/v25/?%s' % (
                urllib.parse.urlencode(url_dic))
        # get_page = get_redirected_resp(url)
        res = retry_get_url(url, headers=headers, timeout=5, proxies=1)

        try:
            get_page = res.json()
            # print(get_page)
        except:
            return None

        if get_page is None:
            return None
        else:
            data = get_page.get("data")
            video_dict = copy.deepcopy(self.video_data)
            fetch_time = int(datetime.datetime.now().timestamp() * 1e3)

            video_dict['url'] = data.get("display_url")
            try:
                try:
                    video_dict['title'] = data.get("h5_extra").get("title")
                except:
                    video_dict['title'] = data.get("share_info").get("title")
            except:
                return None
            video_dict['play_count'] = data.get("video_watch_count")
            video_dict['favorite_count'] = data.get("digg_count")
            video_dict['comment_count'] = data.get("comment_count")
            if not video_dict['comment_count']:
                video_dict['comment_count'] = 0
            video_dict['repost_count'] = data.get("repin_count")
            if not video_dict['repost_count']:
                video_dict['repost_count'] = 0
            video_dict['video_id'] = vid
            try:
                video_dict['releaser'] = data.get("h5_extra").get("name")
            except:
                video_dict['releaser'] = data.get("source")
            try:
                video_dict['releaser_id_str'] = data.get("h5_extra").get("media_user_id")
            except:
                video_dict['releaser_id_str'] = data.get("user_info").get("user_id")
            video_dict['releaserUrl'] = "https://www.toutiao.com/c/user/%s/" % video_dict['releaser_id_str']
            video_dict['releaser_id_str'] = "toutiao_%s" % video_dict['releaser_id_str']
            video_dict['duration'] = data.get("video_duration")
            try:
                if type(video_dict['play_count']) != int:
                    video_dict['play_count'] = data.get("video_detail_info").get("video_watch_count")
            except:
                return None
            try:
                if type(video_dict['duration']) != int:
                    video_dict['duration'] = 0
            except:
                return None
            video_dict['fetch_time'] = fetch_time
            try:
                video_dict['release_time'] = int(int(data.get("h5_extra").get("publish_stamp")) * 1e3)
            except:
                video_dict.pop("release_time")
            for k in video_dict:
                if type(video_dict[k]) == str or type(video_dict[k]) == int:
                    pass
                else:
                    return None
            return video_dict

    def search_page_old(self, keyword, search_pages_max=12,
                        output_to_es_raw=False,
                        output_to_es_register=False,
                        es_index=None,
                        doc_type=None):
        headers_search = {

                "accept": "application/json, text/javascript",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "zh,zh-CN;q=0.9",
                "sec-fetch-mode": "cors",
                "content-type": "application/x-www-form-urlencoded",
                "cookie": r'csrftoken=301d4862d95090ad520f8a54ae360b93; uuid="w:79cdae1ec41c48c9b9cd21255077f629"; _ga=GA1.2.161734802.1557472235; sid_guard="135fa6481579978c777871e6fd64388b|1561603571|15552000|Tue\054 24-Dec-2019 02:46:11 GMT"; tt_webid=6722360764079998478; CNZZDATA1259612802=1042288349-1556155453-%7C1565168219; _ba=BA0.2-20191118-51299-hQirR6SUpyfp1u9bfDab; tt_webid=6722360764079998478; WEATHER_CITY=%E5%8C%97%E4%BA%AC; UM_distinctid=16eef18c3b72df-0f53ff0c8ec3e8-72256d3c-5c030-16eef18c3b8634; s_v_web_id=335bc73471816523ac088af5f1424861; __tasessionId=dofpfb6fv1579159599726',
                "referer": "https://www.toutiao.com/search/?keyword=%s" % urllib.parse.quote(keyword.encode('utf-8')),
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36",
                "x-requested-with": "XMLHttpRequest",
        }
        urls = []
        for page_num in range(0, search_pages_max):
            page_num = page_num * 20
            url = (
                    'https://www.toutiao.com/api/search/content/?aid=24&app_name=web_search&offset={0}&format=json&keyword={1}&autoload=true&count=20&en_qc=1&cur_tab=2&from=video&pd=video&timestamp={2}'.format(
                            page_num, urllib.parse.quote(keyword.encode('utf-8')),
                            int(datetime.datetime.now().timestamp())))
            urls.append(url)
        toutiao_Lst = []
        for search_page_url in urls:
            get_page = requests.get(search_page_url, headers=headers_search)
            if get_page.status_code != 200:
                # retry once
                get_page = requests.get(search_page_url)
                if get_page.status_code != 200:
                    continue
            page_dict = get_page.json()
            if page_dict['data']:
                for one_line in page_dict['data']:
                    try:

                        title = one_line['title']
                        duration = one_line['video_duration']
                        url = one_line['article_url']
                        play_count = one_line['read_count']
                        comment_count = one_line['comment_count']
                        favorite_count = one_line['digg_count']
                        videoid = one_line['id']
                        releaser = one_line['media_name']
                        uid = one_line['user_id']
                        releaserUrl = "https://www.toutiao.com/c/user/%s/" % uid
                        release_time = one_line['publish_time']
                        release_time = int(int(release_time) * 1e3)
                        fetch_time = int(datetime.datetime.now().timestamp() * 1e3)
                        releaser_id = self.find_releaser_id(releaserUrl)
                        D0 = copy.deepcopy(self.video_data)
                        D0['title'] = title
                        D0['duration'] = duration
                        D0['url'] = url
                        D0['play_count'] = play_count
                        D0['comment_count'] = comment_count
                        D0['favorite_count'] = favorite_count
                        D0['video_id'] = videoid
                        D0['releaser'] = releaser
                        D0['releaserUrl'] = releaserUrl
                        D0['release_time'] = release_time
                        D0['releaser_id_str'] = "toutiao_%s" % releaser_id
                        D0['fetch_time'] = fetch_time
                        D0['search_word'] = keyword
                        print(D0)
                        toutiao_Lst.append(D0)
                    except KeyError:
                        # It's totally ok to drop the last return data value.
                        # The search api just return something seems related to search
                        continue
            else:
                break
            if len(toutiao_Lst) >= 100:
                output_result(result_Lst=toutiao_Lst,
                              platform=self.platform,
                              output_to_es_raw=output_to_es_raw,
                              output_to_es_register=output_to_es_register,
                              es_index=es_index,
                              doc_type=doc_type)
                toutiao_Lst.clear()

        if toutiao_Lst != []:
            output_result(result_Lst=toutiao_Lst,
                          platform=self.platform,
                          output_to_es_raw=output_to_es_raw,
                          output_to_es_register=output_to_es_register,
                          es_index=es_index,
                          doc_type=doc_type)

        return toutiao_Lst

    def search_page(self, keyword, search_pages_max=30,
                    output_to_es_raw=False,
                    output_to_es_register=False,
                    es_index=None,
                    doc_type=None):
        self.search_page_old(keyword, search_pages_max=search_pages_max, output_to_es_raw=output_to_es_raw,
                             output_to_es_register=output_to_es_register,
                             es_index=es_index,
                             doc_type=doc_type)

    def find_releaser_id(self, releaserUrl):
        return get_releaser_id(platform=self.platform, releaserUrl=releaserUrl)

    def releaser_page_via_pc(self, releaserUrl,
                             output_to_file=False, filepath=None,
                             releaser_page_num_max=30,
                             output_to_es_raw=False,
                             output_to_es_register=False,
                             push_to_redis=False,
                             es_index=None,
                             doc_type=None,
                             proxy_dic=None):
        """
        If output_to_file=True is passed in, an absolute path string should also
        be passed to filepath parameter. filepath string tells where to put the
        output file. The output file name is assigned automatically, is not supported
        to assign by user.
        """

        headers = {'Host': 'www.365yg.com',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                   'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                   'Accept-Encoding': 'gzip, deflate, br',
                   'Connection': 'keep-alive',
                   'Upgrade-Insecure-Requests': '1',
                   'Cache-Control': 'max-age=0'}
        result_Lst = []
        whether_continue = True
        behot_time = 0
        page_count = 0
        releaser_id = self.find_releaser_id(releaserUrl)
        if releaser_id is None:
            pass
        # self.loggererror.error("%s can't get releaser_id" % releaserUrl)
        else:
            print('releaser_id', releaser_id)
            # self.loggerinfo.info("process on releaser %s" % releaser_id)
            while whether_continue is True and page_count <= releaser_page_num_max:
                releaser_page_url = ('https://www.365yg.com/c/user/article/?user_id='
                                     + releaser_id + '&max_behot_time=' + str(behot_time) +
                                     '&max_repin_time=0&count=20&page_type=0')
                # http://m.365yg.com/video/app/user/home/?to_user_id=73299297129&format=json&max_behot_time=0
                get_page = retry_get_url(releaser_page_url, headers=headers, proxies=proxy_dic)
                if get_page is None:
                    # self.loggererror.error("%s can't get page at page num %s" % (releaserUrl, page_count))
                    print("can't get page at %s" % page_count)
                    continue
                else:
                    page_count += 1
                    try:
                        page_dic = get_page.json()
                    except:
                        page_dic = None
                        # self.loggererror.error('Failed to transfer text to dict on json data url: %s' % releaser_page_url)
                        print('Failed to transfer text to dict on '
                              'json data url: %s' % releaser_page_url)
                        continue
                    video_dic = page_dic['data']
                    whether_continue = page_dic['has_more']
                    behot_time = page_dic['next']['max_behot_time']
                    for line in video_dic:
                        video_dict = copy.deepcopy(self.video_data)
                        # behot_time is different in every video item in the same
                        # releaser page, and will generally descent. The one used
                        # to get next page is the behot_time value from the last
                        # video in the present releaser page.
                        try:
                            video_dict['release_time'] = int(line['behot_time'] * 1e3)
                            video_dict['title'] = line['title']
                            video_dict['url'] = line['display_url']
                            video_dict['releaser'] = line['source']
                            video_id = line['group_id']
                            video_dict['video_id'] = video_id
                            video_dict['url'] = 'http://www.365yg.com/a' + str(video_id) + '/'
                            video_dict['comment_count'] = line['comments_count']
                            duration_str = line['video_duration_str']
                            video_dict['duration'] = trans_duration(duration_str)
                            video_dict['play_count'] = line['video_watch_count']
                        except KeyError as except_msg:
                            # self.loggererror.error('Got KeyError exception: %s at page %s'
                            # % (except_msg, releaserUrl))
                            print('Got KeyError exception: %s at page %s' % (
                                    except_msg, releaserUrl))
                            try:
                                print(duration_str)
                            except:
                                print("can't print duration_str")
                            continue
                        fetch_time = int(datetime.datetime.now().timestamp() * 1e3)
                        video_dict['platform'] = self.platform
                        video_dict['releaser_id_str'] = str(releaser_id)
                        video_dict['fetch_time'] = fetch_time
                        video_dict['releaserUrl'] = releaserUrl
                        result_Lst.append(video_dict)
                        if len(result_Lst) % 100 == 0:
                            output_result(result_Lst, self.platform,
                                          output_to_file=output_to_file,
                                          filepath=filepath,
                                          output_to_es_raw=output_to_es_raw,
                                          output_to_es_register=output_to_es_register,
                                          push_to_redis=push_to_redis,
                                          es_index=es_index,
                                          doc_type=doc_type)
                            result_Lst.clear()
                            print(behot_time)
            #                            self.loggerinfo.info("write %s 100 video_data_dicts into es %s, %s"
            #                                                 % (releaser_id, es_index, doc_type))

            if result_Lst != []:
                output_result(result_Lst, self.platform,
                              output_to_file=output_to_file,
                              filepath=filepath,
                              output_to_es_raw=output_to_es_raw,
                              output_to_es_register=output_to_es_register,
                              push_to_redis=push_to_redis,
                              es_index=es_index,
                              doc_type=doc_type)

    #                self.loggerinfo.info("write %s %s video_data_dicts into es %s, %s"
    #                                     % (releaser_id, len(result_Lst), es_index, doc_type))

    def check_play_count_by_video_page(self, url):
        """
        To check whether the play_count from releaser_page(www.365yg.com) is the
        real play_count show on video_page
        """
        if "toutiao.com" in url:
            video_id_str = ' '.join(re.findall('/group/[0-9]+', url))
            video_id = ' '.join(re.findall('\d+', video_id_str))
            url = 'http://www.365yg.com/a' + video_id
        get_page = get_redirected_resp(url)
        if get_page is None:
            return None
        else:
            page = get_page.text
            find_play_count = re.findall('videoPlayCount: \d+,', page)
            if find_play_count != []:
                play_count = re.findall('\d+', find_play_count[0])[0]
                return int(play_count)
            else:
                print("can't get play_count")

    def get_releaser_image(self, releaserUrl=None, data=None):
        if releaserUrl:
            headers = {'Host': 'www.toutiao.com',
                       'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0',
                       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                       'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                       'Accept-Encoding': 'gzip, deflate, br',
                       'Connection': 'keep-alive',
                       'Upgrade-Insecure-Requests': '1',
                       'Cache-Control': 'max-age=0'}
            proxies = get_proxy(1)
            releaser_id = self.find_releaser_id(releaserUrl)
            releaserUrl = 'https://www.toutiao.com/c/user/' + str(releaser_id) + '/'
            get_page = retry_get_url(releaserUrl, headers=headers, proxies=proxies)
            page = get_page.text
            try:
                releaser_img = re.findall("avtar_img:'(.*)'", data)[0]
                return "http:" + releaser_img
            except:
                print("can't get releaser_img")
        else:
            releaser_img = re.findall("avtar_img:'(.*)'", data)[0]
            return "http:" + releaser_img

    def get_releaser_follower_num(self, releaserUrl):
        releaser_id = self.find_releaser_id(releaserUrl)
        releaserUrl = 'https://www.toutiao.com/c/user/' + str(releaser_id) + '/'
        headers = {
                # 'referer': releaserUrl,
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "accept-encoding": "gzip, deflate",
                "accept-language": "zh,zh-CN;q=0.9",
                "cache-control": "max-age=0",
                # "cookie": 'csrftoken=301d4862d95090ad520f8a54ae360b93; uuid="w:79cdae1ec41c48c9b9cd21255077f629"; _ga=GA1.2.161734802.1557472235; sid_guard="135fa6481579978c777871e6fd64388b|1561603571|15552000|Tue\054 24-Dec-2019 02:46:11 GMT"; tt_webid=6722360764079998478; CNZZDATA1259612802=1042288349-1556155453-%7C1565168219; tt_track_id=c588c87e33b84cd5dd4056cf33200ca4; _ba=BA0.2-20191118-51299-hQirR6SUpyfp1u9bfDab; tt_webid=6722360764079998478; WEATHER_CITY=%E5%8C%97%E4%BA%AC; UM_distinctid=16eef18c3b72df-0f53ff0c8ec3e8-72256d3c-5c030-16eef18c3b8634; s_v_web_id=335bc73471816523ac088af5f1424861; __tasessionId=f2cnfge5z1577959024466',
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.87 Safari/537.36",
        }
        count = 0

        while count < 3:
            try:
                count += 1
                get_page = retry_get_url(releaserUrl, headers=headers, proxies=5)
                page = get_page.text
                follower_num = int(re.findall('\d+', ' '.join(re.findall("fensi:'\d+'", page)))[0])
                print('%s follower number is %s' % (releaserUrl, follower_num))
                releaser_img = self.get_releaser_image(data=page)
                return follower_num, releaser_img
            except:
                print("can't find followers")
                continue
        else:
            return None, None

    def get_releaser_page(self,releaserUrl):
        releaser_id = self.find_releaser_id(releaserUrl)
        releaserUrl = 'https://profile.zjurl.cn/user/profile/homepage/v7/?user_id=%s&request_source=1'% str(releaser_id)
        headers = {
            "accept": "text/javascript, text/html, application/xml, text/xml, */*",
            "accept-encoding": "gzip, deflate",
            "accept-language": "zh,zh-CN;q=0.9",
            "content-type": "application/x-www-form-urlencoded",
            # "cookie": "_ga=GA1.2.820802612.1574645404; SLARDAR_WEB_ID=6678585193337783815; UM_distinctid=16f694e01a8d0-033097fa287e68-5f4e2917-161398-16f694e01a96ab; odin_tt=c3b67a2f796710b8aeda2eea0ff940b66427f774a370e8151a32483f06d120ad1c713efefc2d455cb9ec6178ddbbd1a6; gftoken=NDE2NDU0MzExM3wxNTgyODkwMDI4NTh8fDAGBgYGBgY; CNZZDATA1274386066=1245948952-1578016633-%7C1589879022",
            "referer": releaserUrl,
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1",
            "x-requested-with": "XMLHttpRequest",
        }
        count = 0

        while count < 3:
            try:
                count += 1
                get_page = retry_get_url(releaserUrl, headers=headers, proxies=5,timeout=5)
                page = get_page.json()
                follower_num = page["data"]['followers_count']
                print('%s follower number is %s' % (releaserUrl, follower_num))
                time.sleep(random.random())
                location = page["data"].get('area')
                huozan = page["data"].get("digg_count")
                verify = page["data"].get("'verified_content'")
                signature = page["data"].get('description')
                publish_count = page["data"].get('publish_count')
                signature_type = page["data"].get('media_type')
                dic = {
                        "location":location,
                        "huozan":huozan,
                        "verify":verify,
                        "signature":signature,
                        "follower_num":follower_num,
                        "publish_count":publish_count,
                        "signature_type": signature_type,
                }
                print(dic)
                return dic
            except:
                print("can't find followers")
                continue
        else:
            return None

    def get_releaserUrl_from_video_page(self, url, proxy_dic=None):
        """
        To get releaserUrl from video page
        """
        if "toutiao.com" in url:
            video_id_str = ' '.join(re.findall('/group/[0-9]+', url))
            video_id = ' '.join(re.findall('\d+', video_id_str))
            url = 'http://www.365yg.com/a' + video_id
        get_page = retry_get_url(url, proxies=proxy_dic)
        if get_page is None:
            return None
        else:
            page = get_page.text
            find_releaser_id = re.findall("mediaId: '\d+',", page)
            if find_releaser_id != []:
                releaser_id = re.findall('\d+', ' '.join(find_releaser_id))[0]
                releaserUrl = 'https://www.toutiao.com/c/user/' + str(releaser_id) + '/'
                return releaserUrl
            else:
                return None

    def list_page(self, task_lst,
                  output_to_file=False,
                  filepath=None,
                  page_num_max=20,
                  output_to_es_raw=False,
                  output_to_es_register=False,
                  es_index='crawler-data-raw',
                  doc_type='doc',
                  proxy_dic=None):

        """
        To get video data from list page, it can not be revised to async-crawler
        due to the next page depends on the previous page's data
        """

        cookie = ('tt_webid=6553778542248003086;'
                  'CNZZDATA1259612802=1625670355-1516338642-%7C1527150919;'
                  '_ga=GA1.2.1539151044.1516342895;'
                  '__utma=91863192.1539151044.1516342895.1521092491.1521092491.1;'
                  '__tea_sdk__user_unique_id=6553778542248003086;'
                  '__tea_sdk__ssid=545b4c91-3bd3-4748-831b-6edbf9415b70;'
                  'CNZZDATA1262382642=810628165-1527124428-%7C1529484233;'
                  '__tasessionId=ptjvpftsc1539054420281;'
                  '_gid=GA1.2.1520435477.1539054422')

        headers = {'Host': 'www.365yg.com',
                   'User-Agent': self.random_useragent(),
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                   'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                   'Accept-Encoding': 'gzip, deflate, br',
                   'Cookie': cookie,
                   'Connection': 'keep-alive',
                   'Upgrade-Insecure-Requests': '1',
                   'Cache-Control': 'max-age=0'}

        result_lst = []
        max_behot_time = 0
        page_num = 1
        while page_num <= page_num_max:
            listurl = ('https://www.365yg.com/api/pc/feed/?max_behot_time='
                       + str(max_behot_time) + '&category=video_new&utm_source=toutiao')
            get_page = retry_get_url(listurl, headers=headers, proxies=proxy_dic)
            page_num += 1
            try:
                page_dic = get_page.json()
            except:
                page_dic = {}
            if page_dic == {}:
                max_behot_time = 0
                print("can't get list page")
                continue
            else:
                max_behot_time = page_dic['next']['max_behot_time']
                video_info_lst = page_dic['data']
                for line in video_info_lst:
                    video_dic = copy.deepcopy(self.video_data)
                    title = line['title']
                    video_dic['title'] = line['title']
                    video_dic['data_from'] = 'list_page'
                    video_dic['url'] = 'https://www.365yg.com/a' + line['group_id']
                    try:
                        dura_str = line['video_duration_str']
                        video_dic['duration'] = trans_duration(dura_str)
                    except:
                        video_dic['duration'] = 0
                        print("%s can't get duration" % title)
                    video_dic['releaser'] = line['source']
                    video_dic['releaserUrl'] = 'https://www.365yg.com' + line['media_url']
                    video_dic['release_time'] = int(int(line['behot_time']) * 1e3)
                    try:
                        video_dic['describe'] = line['abstract']
                    except:
                        video_dic['describe'] = ''
                        print("%s can't get describe" % title)
                    try:
                        video_dic['play_count'] = line['video_play_count']
                    except:
                        video_dic['play_count'] = 0
                        print("%s can't get play_count" % title)
                    video_dic['fetch_time'] = int(datetime.datetime.timestamp() * 1e3)
                    result_lst.append(video_dic)
                    if len(result_lst) >= 100:
                        output_result(result_Lst=result_lst,
                                      platform=self.platform,
                                      output_to_file=output_to_file,
                                      filepath=filepath,
                                      output_to_es_raw=output_to_es_raw,
                                      output_to_es_register=output_to_es_register,
                                      es_index=es_index,
                                      doc_type=doc_type)
                        result_lst.clear()
                        print(max_behot_time)
        if result_lst != []:
            output_result(result_Lst=result_lst,
                          platform=self.platform,
                          output_to_file=output_to_file,
                          filepath=filepath,
                          output_to_es_raw=output_to_es_raw,
                          output_to_es_register=output_to_es_register,
                          es_index=es_index,
                          doc_type=doc_type)
        return result_lst

    def get_data_mediaid(self, releaserUrl, releaser_id):
        headers = {
                "Host": "m.toutiao.com",
                "Connection": "keep-alive",
                "Cache-Control": "max-age=0",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": self.random_useragent(),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "zh-CN,zh;q=0.9"
        }
        releaserUrl = "http://m.toutiao.com/profile/%s/#mid=%s" % (releaser_id, releaser_id)
        time.sleep(1)
        res = requests.get(releaserUrl, headers=headers, timeout=5)

        # cookie = requests.utils.dict_from_cookiejar(res.cookies)
        # print(cookie)
        try:
            data_mediaid = re.findall(r'data-mediaid="(\d+)"', res.text)
        except:
            data_mediaid = ""
        if data_mediaid:
            # print(data_mediaid)
            return data_mediaid[0]
        else:
            return False

    def App_releaser_page_video(self, releaserUrl,
                                output_to_file=False,
                                filepath=None,
                                releaser_page_num_max=30,
                                output_to_es_raw=False,
                                output_to_es_register=False,
                                push_to_redis=False,
                                es_index=None,
                                doc_type=None,
                                proxies_num=None):

        result_list = []
        has_more = True
        count = 1
        count_false = 0
        releaser_id = self.find_releaser_id(releaserUrl)
        offset = ""
        self.headers["referer"]="https://profile.zjurl.cn/rogue/ugc/profile/?user_id=%s&request_source=1" % releaser_id
        # vid = "AB5483CA-FCDC-42F1-AFB1-077A1%sDA" % random.randint(100000, 999999)
        # ccid = "F153594D-1310-4984-A4C3-A679D4D%s" % random.randint(10000, 99999)
        # openudid = "5d44f2ea1b74e3731b27e5ed8039ac29f%s" % random.randint(1000000, 9999999)
        # idfa = "E3FC9054-384B-485F-9B4C-936F33D7D%s" % random.randint(100, 999)
        # iid = str(random.randint(104525900000, 104526000000))
        while has_more and count <= releaser_page_num_max:
            # print(str(releaser_id)+str(max_behot_time))
            # js_head = json.loads(get_js(str(releaser_id)+str(max_behot_time)))
            print("get %s video on page %s" % (releaser_id, count))

            # url_dic = {
            #     "visited_uid": releaser_id,
            #     "client_extra_params": '{"playparam":"codec_type:0"}',
            #     "count": "20",
            #     "category": "profile_video",
            #     "offset": offset,
            #     "stream_api_version": "88",
            #     # "client_extra_params": '{"playparam": "codec_type:0"}',
            #     # "media_id": "1649067165440014",
            #     # "resolution": "640*1136",
            #     "ab_feature": "794526,1226546",
            #     # "vid": vid,
            #     "app_name": "news_article",
            #     "channel": "App%20Store",
            #     # "cdid": ccid,
            #     "resolution": "1536*2048",
            #     "device_type": "iPad6,11",
            #     "iid": iid,
            #     # "cdid": ccid,
            #     "ssmix": "a",
            #     "update_version_code": "75410",
            #     "aid": "13",
            #     # "openudid":openudid,
            #     # "idfv": vid,
            #     "os_version": "13.3",
            #     "device_platform": "ipad",
            #     "ab_client": "a1,f2,f7,e1",
            #     "ab_group": "794526,1226546",
            #     "ac": "WIFI",
            #     "version_code": "7.5.4",
            #     # "device_id": random.randint(60000000000, 80000000000),
            #     # "openudid": openudid,
            #     # "idfv": vid,
            #     # "idfa": idfa,
            #     # "iid": "95105525198",
            #     # "device_id": "70149736870",
            #     #     "iid": "94295239614",
            #     #     "device_id": "69418894872",
            # }
            # url_dic = {
            #         # "category": "profile_video",
            #         "visited_uid": releaser_id,
            #         "stream_api_version": "47",
            #         "count": "20",
            #         "offset": offset,
            #         "client_extra_params": r'{}',
            #         "iid": iid,
            #         "ac": "wifi",
            #         # "mac_address": "08:00:27:1F:7E:A0",
            #         "channel": "wap_test_lite_1",
            #         "aid": "35",
            #         "app_name": "news_article_lite",
            #         "version_code": "715",
            #         "version_name": "7.1.5",
            #         "device_platform": "android",
            #         "ab_version": "668903,668905,668907,808414,772541,1378617,668908,668904,668906,1401332,1496418,9289425",
            #         "ab_client": "a1,c2,e1,f2,g2,f7",
            #         "ab_feature": "z1",
            #         "abflag": "3",
            #         "ssmix": "a",
            #         "device_type": "OPPO R11",
            #         "device_brand": "OPPO",
            #         "language": "zh",
            #         "os_api": "22",
            #         "os_version": "5.1.1",
            #         "manifest_version_code": "715",
            #         "resolution": "1920*1080",
            #         "dpi": "401",
            #         "update_version_code": "71504",
            #         "sa_enable": "0",
            #         "tma_jssdk_version": "1.25.4.2",
            #         # "fp": "a_fake_fp",
            #         "rom_version": "coloros__r11-user",
            #         "plugin_state": "30631999",
            #         "as": "ab9db50e485e50977f9db5",
            #         "mas": "ab9db50e485e50977f9db5",
            #
            # }
            # host = random.choice(self.api_list)
            # self.headers["Host"] = host
            # url = "https://{0}.snssdk.com/api/feed/profile/v{1}/?category=profile_video&{2}".format(host,
            #                                                                                         random.randint(5,
            #                                                                                                        10),
            #                                                                                         urllib.parse.urlencode(
            #                                                                                             url_dic))
            # url = """https://{0}.snssdk.com/api/feed/profile/v{1}/?category=profile_video&visited_uid={2}&client_extra_params=&count=20&offset={3}&stream_api_version=88&category=profile_video&version_code=7.6.0&app_name=news_article&channel=App Store&resolution=1536*2048&aid=13&ab_feature=794528&ab_version=765192,857803,660830,1444046,1397712,1434498,662176,801968,1419045,668775,1462526,1190525,1489306,1493796,1439625,1469498,668779,1417599,662099,1477261,1484884,668774,1496422,1427395&ab_group=794528&pos=5pe9vb/x8v788cLx/On47unC7fLuv72nveaXvb29vb/ 8vLv fTz/On4y/zx6Pjuv72nveaXvb29vb29v/Hy8/r06ej5 L 9p72tsZe9vb29vb2/8fzp9Ono fi/vae9rZe9vb294Je9veCX4A==&update_version_code=76014&ac=WIFI&os_version=13.3.1&ssmix=a&device_platform=ipad&iid={4}&ab_client=a1,f2,f7,e1&device_type=iPad6,11""".format(
            #     random.choice(self.api_list), random.randint(5, 10), str(releaser_id), str(offset), iid)
            # url = "https://{0}.snssdk.com/api/feed/profile/v{1}/?category=profile_video&visited_uid={2}&stream_api_version=47&count=20&offset={3}&ac=wifi&channel=wap_test_lite_1&&iid=104525915827&device_id=70787469432&aid=35&app_name=news_article_lite&version_code=715&version_name=7.1.5&device_platform=android&ab_version=668903,668905,668907,808414,772541,1378617,668908,668904,668906,1401332,1496418,928942&ab_client=a1,c2,e1,f2,g2,f7&ab_feature=z1&abflag=3&ssmix=a&device_type=OPPO R11&device_brand=OPPO&language=zh&os_api=22&os_version=5.1.1&manifest_version_code=715&resolution=900*1600&dpi=320&update_version_code=71504&sa_enable=0&fp=a_fake_fp&tma_jssdk_version=1.25.4.2&rom_version=coloros__r11-user 5.1.1 nmf26x 500200210 release-keys&plugin_state=30631999".format(
            #         random.choice(self.api_list), random.randint(5, 10), str(releaser_id), str(offset))
            url = "https://profile.zjurl.cn/api/feed/profile/v1/?category=profile_video&visited_uid={0}&stream_api_version=82&request_source=1&offset={1}&user_id={2}".format(str(releaser_id), str(offset),str(releaser_id))
            # print(url)
            try:
                proxies = get_proxy(proxies_num)
                if proxies:
                    get_page = requests.get(url, headers=self.headers, proxies=proxies, timeout=10)
                else:
                    get_page = requests.get(url, headers=self.headers, timeout=10)
            except:
                continue
            # time.sleep(0.3)
            # print(get_page.text)
            # time.sleep(0.5)
            # format_json = re.match(r"jsonp\d+", get_page.text)
            page_dic = {}
            # print(get_page.text)
            try:
                page_dic = get_page.json()
                if page_dic.get("message") != "success":
                    count_false += 1
                    if count_false < 10:
                        continue
                    else:
                        print("unknow error")
                        break
                # print(get_page)
                # print(page_dic)
                data_list = page_dic.get('data')
                has_more = page_dic.get('has_more')
                offset = str(page_dic.get("offset"))
            except:
                if not page_dic:
                    count_false += 1
                    if count_false >= 10:
                        break
                    else:
                        continue
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
                # print(url)
                proxies = get_proxy(1)
                count_false += 1
                if count_false >= 5:
                    has_more = False
                continue

            else:
                count_false = 0
                count += 1
                for one_video in data_list:
                    # info_str = one_video.get('content')
                    info_dic = json.loads(one_video["content"])
                    video_dic = copy.deepcopy(self.video_data)
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
                    video_dic['fetch_time'] = int(datetime.datetime.now().timestamp() * 1e3)
                    video_dic['releaser_id_str'] = "toutiao_%s" % releaser_id
                    video_dic['video_img'] = self.get_video_image(info_dic)
                    result_list.append(video_dic)
                    if len(result_list) >= 100:
                        # data_count += len(result_list)
                        # print(result_list)
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
            # data_count += len(result_list)
            # print(result_list)
            # print(data_count)
            output_result(result_Lst=result_list,
                          platform=self.platform,
                          output_to_file=output_to_file,
                          filepath=filepath,
                          output_to_es_raw=output_to_es_raw,
                          es_index=es_index,
                          doc_type=doc_type,
                          output_to_es_register=output_to_es_register)

    @staticmethod
    def get_video_image(data):
        video_image_url = ""
        if data.get("large_image_list"):
            video_image_url = data["large_image_list"][0]["url_list"][0]["url"]
            # height = data["large_image_list"][0]["height"]
            # width = data["large_image_list"][0]["width"]
        elif data.get("ugc_video_cover"):
            video_image_url = data["ugc_video_cover"]["url_list"][0]["url"]
            # height = data["ugc_video_cover"]["height"]
            # width = data["ugc_video_cover"]["width"]
        elif data.get("video_detail_info"):
            video_image_url = data["video_detail_info"]["detail_video_large_image"]["url_list"][0]["url"]
            # height = data["video_detail_info"]["detail_video_large_image"]["height"]
            # width = data["video_detail_info"]["detail_video_large_image"]["width"]

        return video_image_url

    def App_releaser_page_all(self, releaserUrl,
                              output_to_file=False,
                              filepath=None,
                              releaser_page_num_max=30,
                              output_to_es_raw=False,
                              output_to_es_register=False,
                              push_to_redis=False,
                              es_index=None,
                              doc_type=None,
                              proxy_dic=None, crawler_type="profile_all", proxies_num=None):

        result_list = []
        has_more = True
        count = 1
        releaser_id = self.find_releaser_id(releaserUrl)
        count_false = 0
        offset = "0"
        self.headers["referer"] = "https://profile.zjurl.cn/rogue/ugc/profile/?user_id=%s&request_source=1" % releaser_id
        # vid = "AB5483CA-FCDC-42F1-AFB1-077A1%sDA" % random.randint(100000, 999999)
        # ccid = "F153594D-1310-4984-A4C3-A679D4D%s" % random.randint(10000, 99999)
        # openudid = "5d44f2ea1b74e3731b27e5ed8039ac29f%s" % random.randint(1000000, 9999999)
        # idfa = "E3FC9054-384B-485F-9B4C-936F33D7D%s" % random.randint(100, 999)
        # iid = str(random.randint(104525900000, 104526000000))
        while has_more and count <= releaser_page_num_max:
            print("get %s article on page %s" % (releaser_id, count))
            # js_head = json.loads(get_js(str(releaser_id)+str(max_behot_time)))
            # url_dic = {
            #         # "category": crawler_type,
            #         "visited_uid": releaser_id,
            #         "client_extra_params": '{"playparam": "codec_type:0"}',
            #         "count": "20",
            #         "offset": offset,
            #         "stream_api_version": "88",
            #         "category": crawler_type,
            #         # "resolution": "640*1136",
            #         # "vid": vid,
            #         "ab_feature": "794526,1226546",
            #         # "vid": vid,
            #         "app_name": "news_article",
            #         "channel": "App Store",
            #         "cdid": ccid,
            #         "resolution": "1536*2048",
            #         "device_type": "iPad6,11",
            #         "ssmix": "a",
            #         "update_version_code": "75410",
            #         "aid": "13",
            #         # "openudid":openudid,
            #         # "idfv": vid,
            #         "os_version": "13.3",
            #         "device_platform": "ipad",
            #         "ab_client": "a1,f2,f7,e1",
            #         "ab_group": "794526,1226546",
            #         "ac": "WIFI",
            #         "version_code": "7.5.4",
            #         "iid": iid,
            #         # "device_id": random.randint(60000000000, 80000000000),
            # }
            # url_dic = {
            #         # "category": "profile_video",
            #         "visited_uid": releaser_id,
            #         "stream_api_version": "47",
            #         "count": "20",
            #         "offset": offset,
            #         "client_extra_params": r'{}',
            #         # "iid": iid,
            #         "ac": "wifi",
            #         # "mac_address": "08:00:27:1F:7E:A0",
            #         "channel": "wap_test_lite_1",
            #         "aid": "35",
            #         "app_name": "news_article_lite",
            #         "version_code": "715",
            #         "version_name": "7.1.5",
            #         "device_platform": "android",
            #         "ab_version": "668903,668905,668907,808414,772541,1378617,668908,668904,668906,1401332,1496418,9289425",
            #         "ab_client": "a1,c2,e1,f2,g2,f7",
            #         "ab_feature": "z1",
            #         "abflag": "3",
            #         "ssmix": "a",
            #         "device_type": "OPPO%20R11",
            #         "device_brand": "OPPO",
            #         "language": "zh",
            #         "os_api": "22",
            #         "os_version": "5.1.1",
            #         "manifest_version_code": "715",
            #         "resolution": "1080*1920",
            #         "dpi": "401",
            #         "update_version_code": "71504",
            #         # "sa_enable": "0",
            #         # "fp": "a_fake_fp",
            #         # "tma_jssdk_version": "1.25.4.2",
            #         # "rom_version": "coloros__r11-user 5.1.1 nmf26x 500200210 release-keys",
            #         # "plugin_state": "30631999",
            #         # "as": "ab9db50e485e50977f9db5",
            #         # "mas": "ab9db50e485e50977f9db5",
            #
            # }
            # url = "http://{0}.snssdk.com/api/feed/profile/v{1}/?category=profile_all&{2}".format(
            #             #         random.choice(self.api_list), random.randint(5, 10), urllib.parse.urlencode(url_dic))
            # url = "https://{0}.snssdk.com/api/feed/profile/v{1}/?category=profile_all&visited_uid={2}&stream_api_version=47&count=20&offset={3}&ac=wifi&channel=wap_test_lite_1&aid=35&app_name=news_article_lite&&iid=104525915827&device_id=70787469432&version_code=715&version_name=7.1.5&device_platform=android&ab_version=668903,668905,668907,808414,772541,1378617,668908,668904,668906,1401332,1496418,928942&ab_client=a1,c2,e1,f2,g2,f7&ab_feature=z1&abflag=3&ssmix=a&device_type=OPPO R11&device_brand=OPPO&language=zh&os_api=22&os_version=5.1.1&manifest_version_code=715&resolution=900*1600&dpi=320&update_version_code=71504&sa_enable=0&fp=a_fake_fp&tma_jssdk_version=1.25.4.2&rom_version=coloros__r11-user 5.1.1 nmf26x 500200210 release-keys&plugin_state=30631999".format(
            #     random.choice(self.api_list), random.randint(5, 10), str(releaser_id), str(offset))
            # url = """https://{0}.snssdk.com/api/feed/profile/v{1}/?category=profile_all&visited_uid={2}&client_extra_params=&count=20&offset={3}&stream_api_version=88&category=profile_all&version_code=7.6.0&app_name=news_article&channel=App Store&resolution=1536*2048&aid=13&ab_feature=794528&ab_version=765192,857803,660830,1444046,1397712,1434498,662176,801968,1419045,668775,1462526,1190525,1489306,1493796,1439625,1469498,668779,1417599,662099,1477261,1484884,668774,1496422,1427395&ab_group=794528&pos=5pe9vb/x8v788cLx/On47unC7fLuv72nveaXvb29vb/ 8vLv fTz/On4y/zx6Pjuv72nveaXvb29vb29v/Hy8/r06ej5 L 9p72tsZe9vb29vb2/8fzp9Ono fi/vae9rZe9vb294Je9veCX4A==&update_version_code=76014&ac=WIFI&os_version=13.3.1&ssmix=a&device_platform=ipad&iid={4}&ab_client=a1,f2,f7,e1&device_type=iPad6,11""".format(
            #     random.choice(self.api_list), random.randint(5, 10), str(releaser_id), str(offset), iid)
            url = "https://profile.zjurl.cn/api/feed/profile/v1/?category=profile_all&visited_uid={0}&stream_api_version=82&request_source=1&offset={1}&user_id={2}&e=md5&operator=CHINA MOBILE_46007&network=WIFI".format(
                str(releaser_id), str(offset), str(releaser_id))

            # print(url)

            proxies = get_proxy(proxies_num=proxies_num)
            try:
                if proxies:
                    get_page = requests.get(url, headers=self.headers, proxies=proxies, timeout=10)
                else:
                    get_page = requests.get(url, headers=self.headers, timeout=10)
            except Exception as e:
                print(e)
                continue
            # print(get_page.text)

            # print(get_page.text)
            # time.sleep(0.5)
            # format_json = re.match(r"jsonp\d+", get_page.text)
            page_dic = {}
            try:
                page_dic = get_page.json()
                data_list = page_dic.get('data')
                if page_dic.get("message") != "success":
                    count_false += 1
                    if count_false < 3:
                        continue
                    else:
                        print("unknow error")
                        break
                has_more = page_dic.get('has_more')
                offset = str(page_dic.get("offset"))
            except:
                if not page_dic:
                    count_false += 1
                    if count_false >= 3:
                        break
                    else:
                        continue
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
                has_more = False
                continue
            else:
                count += 1
                count_false = 0
                for one_video in data_list:
                    # info_str = one_video.get('content')
                    info_dic = json.loads(one_video["content"])
                    if info_dic.get("has_video"):
                        video_dic = copy.deepcopy(self.video_data)
                        video_dic['title'] = info_dic.get('title')
                        video_dic['url'] = info_dic.get('share_url')
                        if video_dic['url'] == "":
                            video_dic['url'] = info_dic.get('url')
                        video_dic['releaser'] = info_dic.get('source')
                        video_dic['releaserUrl'] = releaserUrl
                        release_time = info_dic.get('publish_time')
                        video_dic['release_time'] = int(release_time * 1e3)
                        video_dic['duration'] = info_dic.get('video_duration')
                        video_dic['play_count'] = info_dic.get("video_detail_info").get("video_watch_count")
                        if not video_dic['play_count']:
                            video_dic['play_count'] = info_dic.get("read_count")
                        video_dic['repost_count'] = info_dic.get('forward_info').get('forward_count')
                        video_dic['comment_count'] = info_dic.get('comment_count')
                        video_dic['favorite_count'] = info_dic.get('digg_count')
                        video_dic['video_id_str'] = info_dic.get('item_id')
                        video_dic['fetch_time'] = int(datetime.datetime.now().timestamp() * 1e3)
                        video_dic['releaser_id_str'] = "toutiao_%s" % releaser_id
                        video_dic['video_img'] = self.get_video_image(info_dic)
                        # print(video_dic)
                        for v in video_dic:
                            try:
                                if video_dic[v] is not None:
                                    pass
                                else:
                                    video_dic = self.video_page(video_dic['url'])
                                    if video_dic:
                                        pass
                                    else:
                                        continue
                            except:
                                continue
                        result_list.append(video_dic)
                    elif info_dic.get("abstract") == "":
                        video_url = None
                        video_dic = copy.deepcopy(self.video_data)
                        # print(info_dic)
                        if info_dic.get('article_url'):
                            video_url = info_dic.get('article_url')
                        elif info_dic.get('display_url'):
                            video_url = info_dic.get('display_url')
                        elif info_dic.get('url'):
                            video_url = info_dic.get('url')
                        elif info_dic.get('share_url'):
                            video_url = info_dic.get('share_url')
                        elif info_dic.get("raw_data"):
                            if info_dic.get("raw_data").get("origin_group"):
                                video_url = info_dic.get("raw_data").get("origin_group").get('article_url')
                            elif info_dic.get("raw_data").get("comment_base"):
                                video_url = info_dic.get("raw_data").get("comment_base").get('share').get('share_url')
                            elif info_dic.get("raw_data").get("action"):
                                video_url = "https://m.toutiaoimg.cn/group/%s/" % info_dic.get("raw_data").get(
                                        'group_id')
                                video_dic['video_id'] = info_dic.get("raw_data").get('group_id')
                                video_dic['play_count'] = info_dic.get("raw_data").get("action").get("play_count")
                                video_dic['repost_count'] = info_dic.get("raw_data").get("action").get("share_count")
                                video_dic['comment_count'] = info_dic.get("raw_data").get("action").get('comment_count')
                                video_dic['favorite_count'] = info_dic.get("raw_data").get("action").get('digg_count')
                                video_dic['duration'] = info_dic.get('raw_data').get('video').get("duration")
                                video_dic['title'] = info_dic.get('raw_data').get("title")
                                video_dic['releaser'] = info_dic.get('raw_data').get("user").get("info").get("name")
                                video_dic['releaserUrl'] = releaserUrl
                                video_dic['url'] = video_url
                                video_dic['releaser_id_str'] = "toutiao_%s" % releaser_id
                                video_dic['fetch_time'] = int(datetime.datetime.now().timestamp() * 1e3)
                                video_dic['video_img'] = "http://p1-tt.bytecdn.cn/large/" + info_dic.get(
                                        'raw_data').get('video').get("origin_cover").get("uri")
                                video_dic['release_time'] = int(info_dic.get("raw_data").get("create_time") * 1e3)
                                video_url = None
                        if video_url:
                            video_page_dic = self.video_page(video_url)
                            if video_page_dic:
                                video_dic.update(video_page_dic)
                        result_list.append(video_dic)
                        if len(result_list) >= 100:
                            # data_count += len(result_list)
                            # print(result_list)
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
            # data_count += len(result_list)
            # print(result_list)
            # print(data_count)
            output_result(result_Lst=result_list,
                          platform=self.platform,
                          output_to_file=output_to_file,
                          filepath=filepath,
                          output_to_es_raw=output_to_es_raw,
                          es_index=es_index,
                          doc_type=doc_type,
                          output_to_es_register=output_to_es_register)

    def retry_url(self, url, headers, cookies):
        retry_count = 0
        proxies = get_proxy(1)
        while retry_count < 10:
            # time.sleep(0.2)
            get_page = requests.get(url, headers=headers, allow_redirects=False, cookies=cookies, proxies=proxies)
            page_dic = get_page.json()
            data_list = page_dic.get('data')
            retry_count += 1
            if data_list:
                return page_dic
        else:
            return False

    def releaser_page_pc(self, releaserUrl,
                         output_to_file=False,
                         filepath=None,
                         releaser_page_num_max=30,
                         output_to_es_raw=False,
                         output_to_es_register=False,
                         push_to_redis=False,
                         es_index=None,
                         doc_type=None,
                         proxy_dic=None, **kwargs):
        page_dic = {}
        result_list = []
        has_more = True
        count = 1
        releaser_id = self.find_releaser_id(releaserUrl)
        # print('releaser_id', releaser_id)
        max_behot_time = 0
        data_count = 0
        # print(as_cp_sign)
        headers = {
                "accept": "application/json, text/javascript",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "zh-CN,zh;q=0.9",
                "content-type": "application/x-www-form-urlencoded",
                "x-requested-with": "XMLHttpRequest",
                "Referer": "https://www.toutiao.com/c/user/%s/" % releaser_id,
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36",
                # "cookie":'cookie: tt_webid=6673330506500982276; WEATHER_CITY=%E5%8C%97%E4%BA%AC; UM_distinctid=169c3156bb86b3-00d2e2a0ad50b2-7a1437-161398-169c3156bb9746; tt_webid=6673330506500982276; csrftoken=301d4862d95090ad520f8a54ae360b93; uuid="w:79cdae1ec41c48c9b9cd21255077f629"; CNZZDATA1259612802=281397494-1553752275-https%253A%252F%252Fwww.baidu.com%252F%7C1555306390',
                "cache-control": "max-age=0",
        }
        user_page_url = "https://www.toutiao.com/c/user/%s/" % releaser_id
        user_page = requests.get(user_page_url, headers=headers)
        cookies = user_page.cookies
        while has_more and count <= releaser_page_num_max:
            # print(str(releaser_id)+str(max_behot_time))
            # js_head = json.loads(get_js(str(releaser_id)+str(max_behot_time)))
            # get_as_cp_sign = requests.get(
            #         "http://127.0.0.1:3000/?id=%s&max_behot_time=%s" % (releaser_id, max_behot_time))
            get_as_cp_sign = as_cp(releaser_id, max_behot_time)
            url_dic = {"page_type": "0",
                       "user_id": releaser_id,
                       "max_behot_time": max_behot_time,
                       "count": "20",
                       "as": get_as_cp_sign[0],
                       "cp": get_as_cp_sign[1],
                       # "_signature": as_cp_sign.get('_signature')
                       }

            url = "https://www.toutiao.com/c/user/article/?%s" % urllib.parse.urlencode(url_dic)
            get_page = requests.get(url, headers=headers, cookies=cookies)
            # time.sleep(0.5)
            # format_json = re.match(r"jsonp\d+", get_page.text)
            page_dic = {}
            try:
                page_dic = get_page.json()
                data_list = page_dic.get('data')
                has_more = page_dic.get('has_more')
                max_behot_time = str(page_dic.get("next")["max_behot_time"])
            except:
                data_list = self.retry_url(url, headers, cookies)
                if data_lis:
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
                continue
            else:
                count += 1
                for one_video in data_list:
                    max_behot_time = str(page_dic.get("next")["max_behot_time"])
                    # info_str = one_video.get('content')
                    info_dic = one_video
                    video_dic = copy.deepcopy(self.video_data)
                    video_dic['title'] = info_dic.get('title')
                    video_dic['url'] = info_dic.get('source_url')
                    video_dic['releaser'] = info_dic.get('source')
                    video_dic['releaserUrl'] = releaserUrl
                    release_time = info_dic.get('behot_time')
                    video_dic['release_time'] = int(release_time * 1e3)
                    video_dic['duration'] = self.t2s(info_dic.get('video_duration_str'))
                    video_dic['play_count'] = info_dic.get('detail_play_effective_count')
                    video_dic['repost_count'] = info_dic.get('forward_info').get('forward_count')
                    # video_dic['comment_count'] = info_dic.get('comment_count')
                    # video_dic['favorite_count'] = info_dic.get('digg_count')
                    # video_dic['video_id'] = info_dic.get('item_id')
                    # video_dic['fetch_time'] = int(time.time() * 1e3)
                    result_list.append(video_dic)
                    if len(result_list) >= 100:
                        data_count += len(result_list)
                        print(result_list)
                        # output_result(result_Lst=result_list,
                        #               platform=self.platform,
                        #               output_to_file=output_to_file,
                        #               filepath=filepath,
                        #               output_to_es_raw=output_to_es_raw,
                        #               es_index=es_index,
                        #               doc_type=doc_type,
                        #               output_to_es_register=output_to_es_register)
                        result_list.clear()
        if result_list != []:
            data_count += len(result_list)
            # print(result_list)
            print(data_count)
            # output_result(result_Lst=result_list,
            #               platform=self.platform,
            #               output_to_file=output_to_file,
            #               filepath=filepath,
            #               output_to_es_raw=output_to_es_raw,
            #               es_index=es_index,
            #               doc_type=doc_type,
            #               output_to_es_register=output_to_es_register)

    # @logged
    def releaser_page_old(self, releaserUrl,
                          output_to_file=False,
                          filepath=None,
                          releaser_page_num_max=30,
                          output_to_es_raw=False,
                          output_to_es_register=False,
                          push_to_redis=False,
                          es_index=None,
                          doc_type=None,
                          proxy_dic=None):
        result_list = []
        has_more = True
        offset = 0
        count = 1
        releaser_id = self.find_releaser_id(releaserUrl)
        headers = {'Host': 'is.snssdk.com',
                   'Connection': 'keep-alive',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0',
                   'Accept-Encoding': 'gzip, deflate, br',
                   'Upgrade-Insecure-Requests': '1',
                   'Cache-Control': 'max-age=0',
                   'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                   # 'Cookie': 'odin_tt=36c67e1135e11981703d797f6246957dcc05964f811721d0c77bcf091fc7484db5698ef9ca767558ea9f1b643ad9e3ac; UM_distinctid=167c9e978c419c-0107b7bccfdb688-143a7540-1fa400-167c9e978c54a2; CNZZDATA1274386066=172017830-1545278343-%7C1545278343'
                   }
        while has_more is True and count <= releaser_page_num_max and offset is not None:
            url_dic = {"category": "profile_video",
                       "visited_uid": releaser_id,
                       "offset": offset}
            url = urllib.parse.urlencode(url_dic)
            url = "https://i.snssdk.com/api/feed/profile/v1/?%s" % urllib.parse.urlencode(url_dic)
            print(url)
            get_page = requests.get(url, headers=headers)
            print("process on releaser %s page %s" % (releaser_id, offset))
            page_dic = get_page.json()
            data_list = page_dic['data']
            has_more = page_dic.get('has_more')
            offset = page_dic.get('offset')
            if has_more is None:
                has_more = False
            if data_list == []:
                print("no data in releaser %s page %s" % (releaser_id, count))
                count += 1
                continue
            else:
                count += 1
                for one_video in data_list:
                    info_str = one_video.get('content')
                    info_dic = json.loads(info_str)
                    video_dic = copy.deepcopy(self.video_data)
                    video_dic['title'] = info_dic.get('title')
                    video_dic['url'] = info_dic.get('display_url')
                    video_dic['releaser'] = info_dic.get('source')
                    video_dic['releaserUrl'] = releaserUrl
                    release_time = info_dic.get('publish_time')
                    video_dic['release_time'] = int(release_time * 1e3)
                    video_dic['duration'] = info_dic.get('video_duration')
                    video_dic['play_count'] = info_dic.get('video_detail_info').get('video_watch_count')
                    video_dic['repost_count'] = info_dic.get('forward_info').get('forward_count')
                    video_dic['comment_count'] = info_dic.get('comment_count')
                    video_dic['favorite_count'] = info_dic.get('digg_count')
                    video_dic['video_id'] = info_dic.get('item_id')
                    video_dic['fetch_time'] = int(datetime.datetime.now().timestamp() * 1e3)
                    result_list.append(video_dic)
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

    def web_releaser_page(self, releaserUrl,
                          output_to_file=False,
                          filepath=None,
                          releaser_page_num_max=30,
                          output_to_es_raw=False,
                          output_to_es_register=False,
                          push_to_redis=False,
                          es_index=None,
                          doc_type=None,
                          proxy_dic=None):
        page_dic = {}
        result_list = []
        has_more = 1
        count = 1
        releaser_id = self.find_releaser_id(releaserUrl)
        print('releaser_id', releaser_id)

        max_behot_time = ""
        count_callback = 3
        data_count = 0
        media_id = self.get_data_mediaid(releaserUrl, releaser_id)
        if not media_id:
            media_id = releaser_id

        headers = {
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "zh-CN,zh;q=0.9",
                "cache-control": "max-age=0",
                "upgrade-insecure-requests": "1",
                "Referer": "http://m.toutiao.com/profile/%s/" % releaser_id,
                "User-Agent": self.random_useragent(),
        }

        while has_more == 1 and count <= releaser_page_num_max:
            eas, ecp = getHoney()
            url_dic = {"page_type": "0",
                       "uid": releaser_id,
                       "max_behot_time": max_behot_time,
                       "media_id": media_id,
                       "output": "json",
                       "is_json": "1",
                       "count": "20",
                       "from": "user_profile_app",
                       "version": "2",
                       "as": eas,
                       "cp": ecp,
                       "callback": "jsonp%s" % count_callback,
                       }

            url = "https://www.toutiao.com/pgc/ma/?%s" % urllib.parse.urlencode(url_dic)
            get_page = requests.get(url, headers=headers, allow_redirects=False)
            # time.sleep(0.5)
            # format_json = re.match(r"jsonp\d+", get_page.text)
            page_dic = {}
            if get_page.text[0:4] == "json":
                get_page_text = (get_page.text[len(url_dic["callback"]) + 1:-1])
                # print(res)0
            try:
                page_dic = json.loads(r"%s" % get_page_text)
                data_list = page_dic.get('data')
                has_more = page_dic.get('has_more')
                max_behot_time = str(page_dic.get("next").get("max_behot_time"))
            except:
                print("json失败 ", get_page.text)
                data_list = []
                has_more = False
            # offset = page_dic.get('offset')
            if has_more is None:
                has_more = False
            if data_list == []:
                print("no data in releaser %s page %s" % (releaser_id, count))
                print(page_dic)
                print(url)
                count += 1
                continue
            else:
                count += 1
                count_callback += 1
                for one_video in data_list:

                    # info_str = one_video.get('content')
                    info_dic = one_video
                    video_dic = copy.deepcopy(self.video_data)
                    video_dic['title'] = info_dic.get('title')
                    video_dic['url'] = info_dic.get('source_url')
                    print(info_dic.get('source_url'))
                    video_dic['releaser'] = info_dic.get('source')
                    video_dic['releaserUrl'] = releaserUrl
                    release_time = info_dic.get('publish_time')
                    video_dic['release_time'] = int(release_time * 1e3)
                    video_dic['duration'] = self.t2s(info_dic.get('video_duration_str'))
                    video_dic['play_count'] = info_dic.get('detail_play_effective_count')
                    # video_dic['repost_count'] = info_dic.get('forward_info').get('forward_count')
                    video_dic['comment_count'] = info_dic.get('comment_count')
                    video_dic['favorite_count'] = info_dic.get('digg_count')
                    video_dic['video_id'] = info_dic.get('item_id')
                    video_dic['fetch_time'] = int(datetime.datetime.now().timestamp() * 1e3)
                    result_list.append(video_dic)
                    if len(result_list) >= 100:
                        # data_count += len(result_list)
                        # print(result_list)
                        # data_count += len(result_list)
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
            # data_count += len(result_list)
            # print(result_list)
            # print(data_count)
            print(result_list)
            output_result(result_Lst=result_list,
                          platform=self.platform,
                          output_to_file=output_to_file,
                          filepath=filepath,
                          output_to_es_raw=output_to_es_raw,
                          es_index=es_index,
                          doc_type=doc_type,
                          output_to_es_register=output_to_es_register)

    # @logged
    def releaser_page(self, releaserUrl,
                      output_to_file=False,
                      filepath=None,
                      releaser_page_num_max=80,
                      output_to_es_raw=False,
                      output_to_es_register=False,
                      push_to_redis=False,
                      es_index=None,
                      doc_type=None,
                      proxies_num=None):
        self.App_releaser_page_video(releaserUrl, output_to_file, filepath, releaser_page_num_max, output_to_es_raw,
                                     output_to_es_register, push_to_redis, es_index, doc_type, proxies_num)
        self.App_releaser_page_all(releaserUrl, output_to_file, filepath, releaser_page_num_max, output_to_es_raw,
                                   output_to_es_register, push_to_redis, es_index, doc_type, proxies_num)

    @staticmethod
    def t2s(t):
        if t:
            if len(t) == 5:
                t = str(t)
                m, s = t.strip().split(":")
                return float(m) * 60 + float(s)
            elif len(t) == 8:
                t = str(t)
                h, m, s = t.strip().split(":")
                return float(h) * 3600 + float(m) * 60 + float(s)
        else:
            return 0

    @staticmethod
    def random_useragent():
        agent_lis = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36",
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
                "Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50",
                "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; en) Opera 9.50",
                "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0",
                "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10",
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2",
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
                "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16",
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36",
                "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
                "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
                "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
                "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
                "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
                "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0",
                "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; SE 2.X MetaSr 1.0)",
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Maxthon/4.4.3.4000 Chrome/30.0.1599.101 Safari/537.36",
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 UBrowser/4.0.3214.0 Safari/537.36"
        ]
        return agent_lis[random.randrange(0, len(agent_lis))]


if __name__ == '__main__':
    data_lis = ["https://www.toutiao.com/c/user/85676020889/#mid=1593814181797896"]
    data_lis = ["https://www.toutiao.com/c/user/6113709817/#mid=6113817558",
                "https://www.toutiao.com/c/user/3688888283/#mid=3689528443",
                "https://www.toutiao.com/c/user/4188615746/#mid=4273783271"]
    # data_lis = ["https://www.toutiao.com/c/user/6027579671/#mid=6217730861","http://www.toutiao.com/c/user/61621115551/#mid=1569627833237506"]
    # data_lis = ["https://www.toutiao.com/c/user/57767591798/#mid=1561719930544130"]
    miaopai_list = []
    test = Crawler_toutiao()
    # test.releaser_page(data_lis[0])
    # res = test.video_page("https://www.ixigua.com/i6701478014242259463/")
    # print(res)
    for u in data_lis:
        #     # # test.releaser_page(u)
        # test.App_releaser_page_video(u, output_to_es_raw=True, es_index='crawler-data-raw',
        #                              doc_type='doc',
        #                              releaser_page_num_max=3, proxies_num=0)

        test.get_releaser_page(u)
        # test.App_releaser_page_all(u, output_to_es_raw=True, es_index='crawler-data-raw',
            #                            doc_type='doc',
            #                            releaser_page_num_max=3, proxies_num=1))
    # test.releaser_page(u)
    # test.search_page("北京国安")
