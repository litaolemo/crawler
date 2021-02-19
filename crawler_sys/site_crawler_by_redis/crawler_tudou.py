# -*- coding: utf-8 -*-
"""
Created on Sat Feb 24 14:19:00 2018

@author: fangyucheng
"""

import os
import re
import asyncio
import time
import copy
import requests
import datetime
import aiohttp
import urllib
try:
    from crawler_sys.framework.func_get_releaser_id import *
except:
    from func_get_releaser_id import *
from bs4 import BeautifulSoup
from multiprocessing import Pool
from multiprocessing import Process
from crawler.crawler_sys.framework.video_fields_std import Std_fields_video
from crawler.crawler_sys.utils.output_results import retry_get_url
from crawler.crawler_sys.utils.output_results import output_result
from crawler.crawler_sys.utils.trans_str_play_count_to_int import trans_play_count
from crawler.crawler_sys.utils.trans_strtime_to_timestamp import trans_strtime_to_timestamp
from crawler.crawler_sys.utils.trans_duration_str_to_second import trans_duration
from crawler.crawler_sys.utils import connect_with_redis
from crawler.crawler_sys.utils.util_logging import logged
from crawler.crawler_sys.proxy_pool.func_get_proxy_form_kuaidaili import get_proxy
#
#logging = output_log(page_category='crawler',
#                     program_info='tudou',)

class Crawler_tudou():

    def __init__(self, timeout=None, platform='new_tudou'):
        if timeout == None:
            self.timeout = 10
        else:
            self.timeout = timeout
        self.platform = platform
        self.TotalVideo_num = None
        self.midstepurl = None
        std_fields = Std_fields_video()
        self.video_data = std_fields.video_data
        self.video_data['platform'] = self.platform
        unused_key_list = ['channel', 'describe', 'repost_count', 'isOriginal']
        for key in unused_key_list:
            self.video_data.pop(key)
        self.list_page_url_lst = ["http://www.tudou.com/api/getfeeds?secCateId=10016&utdid=T8v9EQPOimUCAXL%2FAz0YrDOB&page_size=24",
                                  "http://www.tudou.com/api/getfeeds?secCateId=10195&utdid=XA2EFIGslWoCAWp4y3KXcZh7&page_size=24",
                                  "http://www.tudou.com/api/getfeeds?secCateId=622736331&utdid=XA2EFIGslWoCAWp4y3KXcZh7&page_size=24",
                                  "http://www.tudou.com/api/getfeeds?secCateId=622769673&utdid=XA2EFIGslWoCAWp4y3KXcZh7&page_size=24",
                                  "http://www.tudou.com/api/getfeeds?secCateId=10116&utdid=XA2EFIGslWoCAWp4y3KXcZh7&page_size=24",
                                  "http://www.tudou.com/api/getfeeds?secCateId=622621940&utdid=XA2EFIGslWoCAWp4y3KXcZh7&page_size=24",
                                  "http://www.tudou.com/api/getfeeds?secCateId=10198&utdid=XA2EFIGslWoCAWp4y3KXcZh7&page_size=24",
                                  "http://www.tudou.com/api/getfeeds?secCateId=622336449&utdid=XA2EFIGslWoCAWp4y3KXcZh7&page_size=24",
                                  "http://www.tudou.com/api/getfeeds?secCateId=10051&utdid=XA2EFIGslWoCAWp4y3KXcZh7&page_size=24"]

    def video_page(self, url):
        video_info = copy.deepcopy(self.video_data)
        get_page = requests.get(url)
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        try:
            video_info['title'] = soup.find('h1', {'class': 'td-playbase__title'}).span.text
        except:
            video_info['title'] = None
        try:
            video_info['releaser'] = soup.find('a',{'class':'td-play__userinfo__name'}).text
        except:
            video_info['releaser'] = None
        try:
            midsteptime = soup.find('div',{'class':
                                           'td-play__videoinfo__details-box__time'}).text[:-2]
            video_info['release_time'] = int(datetime.datetime.strptime(midsteptime,
                                                                        '%Y-%m-%d %H:%M:%S').timestamp()*1e3)
        except:
            video_info['release_time'] = None
        try:
            video_info['releaserUrl'] = soup.find("a", {"class": "td-play__userinfo__name"})['href']
        except:
            video_info['releaserUrl'] = None
        try:
            find_play_count = ' '.join(re.findall('total_vv.*stripe_bottom', page))
            replace_comma_pcnt = find_play_count.replace(',', '')
            play_count_str = ' '.join(re.findall('total_vv":"\d+', replace_comma_pcnt))
            video_info['play_count'] = int(' '.join(re.findall('\d+', play_count_str)))
        except:
            video_info['play_count'] = 0
        try:
            find_comment_count = ' '.join(re.findall('total_comment.*recommend', page))
            replace_comma_ccnt = find_comment_count.replace(',', '')
            comment_count_str =  ' '.join(re.findall('total_comment":"\d+', replace_comma_ccnt))
            video_info['comment_count'] = int(' '.join(re.findall('\d+', comment_count_str)))
        except:
            video_info['comment_count'] = 0
        try:
            find_dura = re.findall('stripe_bottom":"\d+:\d+', page)
            dura_str = ' '.join(find_dura).split('":"')[-1]
            video_info['duration'] = trans_duration(dura_str)
        except:
            video_info['duration'] = 0
        video_info['fetch_time'] = int(time.time()*1e3)
        video_info['url'] = url
        print("get video data at %s" % url)
        return video_info

    def releaser_page_web(self, releaserUrl,
                          output_to_file=False,
                          filepath=None,
                          releaser_page_num_max=30,
                          output_to_es_raw=False,
                          es_index=None,
                          doc_type=None,
                          output_to_es_register=False,
                          push_to_redis=False, proxies_num=None):
        releaser_id = self.get_releaser_id(releaserUrl)
        # releaser = self.get_releaser_name(releaserUrl)
        releaserUrl = 'https://id.tudou.com/i/%s/videos' % releaser_id
        json_headers = {
                "accept": "application/json, text/javascript, */*; q=0.01",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "zh,zh-CN;q=0.9",
                # "cookie": "cna=W99aFOvX+QACAXL4fBJI3rAw; __ysuid=1541219939103JPW; ykss=e93bad5ef9c26af71c8e7ee5; P_ck_ctl=47F163FE35A5B1B2E479B158A12376A7; __ayvstp=16; __aysvstp=16; _zpdtk=ecd18a6d5d86a28b786b653356133cfb606dd1dc; isg=BOzsOnpUnhIGhYq8YxHgZ36EvcoepZBPH_JJJ0Yt-Rc6UY5bbrVJ3rr3dxdpWcin",
                "referer": releaserUrl,
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1",
                "x-csrf-token": "ecd18a6d5d86a28b786b653356133cfb606dd1dc",
                "x-requested-with": "XMLHttpRequest",
        }
        json_cookies = {
                "cna": "W99aFOvX+QACAXL4fBJI3rAw",
                "__ysuid": "1541219939103JPW",
                "ykss": "e93bad5ef9c26af71c8e7ee5",
                "P_ck_ctl": "47F163FE35A5B1B2E479B158A12376A7",
                "__ayvstp": "16",
                "__aysvstp": "16",
                "_zpdtk": "ecd18a6d5d86a28b786b653356133cfb606dd1dc",
                "isg": "BOzsOnpUnhIGhYq8YxHgZ36EvcoepZBPH_JJJ0Yt-Rc6UY5bbrVJ3rr3dxdpWcin",
        }
        firsh_page_headers = {

                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "zh,zh-CN;q=0.9",
                # "cookie": "cna=W99aFOvX+QACAXL4fBJI3rAw; __ysuid=1541219939103JPW; ykss=e93bad5ef9c26af71c8e7ee5; P_ck_ctl=47F163FE35A5B1B2E479B158A12376A7; __ayvstp=16; __aysvstp=16; _zpdtk=9053e5d58ee0c51b1f3da8008dd4bda164ecd846; isg=BHl5FRo0A8WDkd_DnlItMBsXiOVThm042sF8-Juu9KAfIpu049ZUCb80oCjUmgVw",
                "referer": releaserUrl,
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "same-origin",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1",

        }
        first_page_res = retry_get_url(releaserUrl, headers=firsh_page_headers, proxies=proxies_num)
        json_cookies.update(dict(first_page_res.cookies))
        user_id = re.findall('uid="(\d+)"', first_page_res.text)[0]
        zptk_url = "https://id.tudou.com/i/h5/id_%s/playlisttab?uid=%s" % (user_id, user_id)
        playlisttab_res = retry_get_url(zptk_url, headers=json_headers, proxies=proxies_num, cookies=json_cookies)
        # print(dict(playlisttab_res.cookies))
        json_cookies.update(dict(playlisttab_res.cookies))
        json_headers["x-csrf-token"] = dict(playlisttab_res.cookies)["_zpdtk"]
        count = 1
        retry_time = 0
        result_list = []

        self.video_data['releaserUrl'] = releaserUrl

        print("working on releaser_id: %s" % (releaser_id))
        while count <= releaser_page_num_max and retry_time < 5:
            proxies = get_proxy(proxies_num)
            api_url = 'https://id.tudou.com/i/h5/id_%s/videos?ajax=1&pn=%s&pl=20' % (user_id, count)
            print(api_url)
            if proxies:
                get_page = requests.get(api_url, headers=json_headers, proxies=proxies, timeout=3, cookies=json_cookies)
            else:
                get_page = requests.get(api_url, headers=json_headers, timeout=3, cookies=json_cookies)
            _zpdtk = dict(get_page.cookies)
            json_cookies.update(_zpdtk)
            # print(dict(get_page.cookies))
            json_headers["x-csrf-token"] = _zpdtk["_zpdtk"]
            page_dic = get_page.json()
            releaser_page_num_max = page_dic["page"]["pz"]
            releaser = page_dic['channelOwnerInfo']["data"]["nickname"]
            #            has_more = page_dic.get('has_more')
            try:
                data_list = page_dic['data']["data"]
                time.sleep(0.25)
            except:
                retry_time += 1
                time.sleep(0.25)
                print("no more data at  page: %s try_time: %s" % (count, retry_time))
                continue
            if data_list == []:
                retry_time += 1
                time.sleep(0.25)
                print("no more data at page: %s try_time: %s" % (count, retry_time))
                continue
            else:
                retry_time = 0
                print("get data at page: %s" % (count))
                count += 1
                for info_dic in data_list:
                    video_info = copy.deepcopy(self.video_data)
                    video_info['video_id'] = info_dic["videoid"]
                    video_info['title'] = info_dic["title"]
                    video_info['releaser'] = releaser
                    video_info['url'] = 'https://video.tudou.com/v/%s.html' % info_dic["videoid"]
                    video_info['duration'] = int(info_dic.get('seconds') / 1e3)
                    video_info['releaser_id_str'] = "new_tudou_%s" % (releaser_id)
                    video_info['comment_count'] = int(info_dic.get('total_comment'))
                    video_info['favorite_count'] = int(info_dic.get('total_up'))
                    # favorite_count in database means 点赞数, while in web page the factor
                    # named praiseNumber
                    # in web page facorite_count means 收藏数
                    video_info['video_img'] = info_dic.get('thumburl')
                    video_info['play_count'] = info_dic.get('total_vv')
                    video_info['release_time'] = int(info_dic.get('publishtime') * 1e3)
                    #                            print(video_info['release_time'])
                    # if '天前' in release_time_str:
                    #     video_info['release_time'] = self.video_page(video_info['url'])['release_time']
                    # else:
                    #     video_info['release_time'] = trans_strtime_to_timestamp(input_time=release_time_str,
                    #                                                             missing_year=True)
                    video_info['fetch_time'] = int(time.time() * 1e3)
                    yield video_info

    def search_page(self, keyword, search_pages_max=30,
                    output_to_es_raw=False,
                    output_to_es_register=False,
                    es_index=None,
                    doc_type=None):
        search_data_Lst=[]
        url=('http://www.soku.com/nt/search/q_'
             + keyword
             + '_orderby_2_limitdate_0?spm=a2h0k.8191414.0.0&site=14&page={}'
                 .format(str(i)) for i in range(1, search_pages_max+1))
        for urls in url:
            print(urls)
            get_page = requests.get(urls)
            page = get_page.text
            soup = BeautifulSoup(page, 'html.parser')
            potato = soup.find_all("div", { "class" : "v" })
            for data_line in potato:
                dura = data_line.find("span", { "class" : "v-time"}).text
                duration_str=dura
                dl = duration_str.split(':')
                dl_int = []
                for v in dl:
                    v = int(v)
                    dl_int.append(v)
                if len(dl_int) == 2:
                    duration = dl_int[0]*60 + dl_int[1]
                else:
                    duration = dl_int[0]*3660+dl_int[1]*60+dl_int[2]
                url = data_line.find('div',{'class':'v-meta-title'}).a['href']
                url = 'http:'+url
                one_video_dic = self.video_page(url)
                one_video_dic['url'] = url
                one_video_dic['duration']=duration
                search_data_Lst.append(one_video_dic)
                print('get page done')

                if len(search_data_Lst) >= 100:
                    output_result(result_Lst=search_data_Lst,
                                  platform=self.platform,
                                  output_to_es_raw=output_to_es_raw,
                                  output_to_es_register=output_to_es_register,
                                  es_index=es_index,
                                  doc_type=doc_type)
                    search_data_Lst.clear()

        if search_data_Lst != []:
            output_result(result_Lst=search_data_Lst,
                          platform=self.platform,
                          output_to_es_raw=output_to_es_raw,
                          output_to_es_register=output_to_es_register,
                          es_index=es_index,
                          doc_type=doc_type)

        return search_data_Lst


    def process_one_video(self, line):
        video_info = copy.deepcopy(self.video_data)
        try:
            video_info['title'] = line.find('a', {'target':'video'})['title']
        except:
            video_info['title'] = None
        try:
            url = line.find('a', {'target':'video'})['href']
            video_info['url'] = 'https:' + url
        except:
            video_info['url'] = None
        try:
            play_count_str = line.find('span', {'class': 'v-num'}).text
            video_info['play_count'] = trans_play_count(play_count_str)
        except:
            video_info['play_count'] = 0
#            logging.warning("can't get play_count at page %s" % video_info['url'])
        try:
            release_time_str = line.find('span', {'class': 'v-publishtime'}).text
            video_info['release_time'] = trans_strtime_to_timestamp(input_time=release_time_str,
                                                                     missing_year=True)
        except:
            release_time_str = 0
#            logging.warning("can't get release_time at page %s" % video_info['url'])
        try:
            dura_str = line.find('span', {'class': 'v-time'}).text
            video_info['duration'] = trans_duration(dura_str)
        except:
            video_info['duration'] = 0
#            logging.warning("can't get duration at page %s" % video_info['url'])
        fetch_time = int(time.time()*1e3)
        video_info['fetch_time'] = fetch_time
        return video_info


    def get_releaser_id(self, releaserUrl):
        return get_releaser_id(platform=self.platform,releaserUrl=releaserUrl)


    def releaser_page_via_pc(self, releaserUrl,
                             output_to_file=False,
                             filepath=None,
                             releaser_page_num_max=1000,
                             output_to_es_raw=False,
                             output_to_es_register=False,
                             push_to_redis=False,
                             es_index=None,
                             doc_type=None):
        """
        input releaserUrl must be strict as https://id.tudou.com/i/UMjc5MzI5NDA==/videos?
        end with /videos otherwise when scrolling it will make mistakes
        """
        releaser_id = self.get_releaser_id(releaserUrl)
        print("working on releaser: %s" % releaser_id)
        releaserUrl = 'https://id.tudou.com/i/%s/videos' % releaser_id
        result_lst = []
        get_page = retry_get_url(releaserUrl)
        get_page.encoding = 'utf-8'
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        try:
            releaser = soup.find('div', {'class': 'user-name'}).a.text
        except:
            releaser = None
        try:
            total_video_num_str = soup.find('div', {'class': 'title'}).span.text
            total_video_num = total_video_num_str.replace('(', '').replace(')', '').replace(',', '')
            total_video_num = trans_play_count(total_video_num)
        except:
            print(releaserUrl)
        if total_video_num%50 == 0:
            total_page_num = int(total_video_num/50)
        else:
            total_page_num = int(total_video_num/50) + 1
        if releaser_page_num_max > total_page_num:
            releaser_page_num_max = total_page_num
        print("releaser page num max is %s" % releaser_page_num_max)
        video_lst = soup.find_all('div', {'class': 'v'})
        for line in video_lst:
            video_info = self.process_one_video(line)
            video_info['releaserUrl'] = releaserUrl
            video_info['releaser'] = releaser
            result_lst.append(video_info)
        if releaser_page_num_max >= 2:
            page_num = 2
            try:
                partial_releaserUrl = soup.find('li',{'class':'next'}).a['href']
                new_releaserUrl = 'https://id.tudou.com%s' % partial_releaserUrl
            except:
                print(new_releaserUrl)
            while page_num <= releaser_page_num_max:
                get_page = retry_get_url(new_releaserUrl)
                get_page.encoding = 'utf-8'
                page = get_page.text
                soup = BeautifulSoup(page, 'html.parser')
                if page_num != releaser_page_num_max:
                    try:
                        new_releaserUrl = 'https://id.tudou.com' + soup.find('li',{'class':'next'}).a['href']
                    except:
                        new_releaserUrl = ('https://id.tudou.com/i/%s/videos?order=1&page=%s' % (releaser_id, page_num))
                video_lst = soup.find_all('div', {'class': 'v'})
                for line in video_lst:
                    video_info = self.process_one_video(line)
                    video_info['releaserUrl'] = releaserUrl
                    video_info['releaser'] = releaser
                    result_lst.append(video_info)
                print('get page %s list length is %s' % (page_num, len(result_lst)))
                page_num += 1
                output_result(result_Lst=result_lst,
                              platform=self.platform,
                              output_to_file=output_to_file,
                              filepath=filepath,
                              output_to_es_raw=output_to_es_raw,
                              es_index=es_index,
                              doc_type=doc_type,
                              output_to_es_register=output_to_es_register,
                              push_to_redis=push_to_redis)
                result_lst.clear()
        if result_lst != []:
            output_result(result_Lst=result_lst,
                          platform=self.platform,
                          output_to_file=output_to_file,
                          filepath=filepath,
                          output_to_es_raw=output_to_es_raw,
                          output_to_es_register=output_to_es_register,
                          push_to_redis=push_to_redis,
                          es_index=es_index,
                          doc_type=doc_type)


    def get_releaser_follower_num(self, releaserUrl):
        get_page = requests.get(releaserUrl)
        get_page.encoding = 'utf-8'
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        try:
            follower_str = soup.find('li',{'class':'snum'}).em.text
            follower_num = trans_play_count(follower_str)
            print('%s follower number is %s' % (releaserUrl, follower_num))
            releaser_img = self.get_releaser_image(data=soup)
            return follower_num,releaser_img
        except:
            print("can't can followers")

    def releaser_video_sum(self, releaserUrl):
        get_page = retry_get_url(releaserUrl)
        get_page.encoding = 'utf-8'
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        total_video_num_str = soup.find('div', {'class':'title'}).span.text
        total_video_num = total_video_num_str.replace('(', '').replace(')', '').replace(',', '')
        total_video_num = trans_play_count(total_video_num)
        return total_video_num

    def start_list_page(self, task_list,
                        retry_times=20):
        url_lst = []
        count = 0
        while count < retry_times:
            count += 1
            for list_url in task_list:
                get_page = requests.get(list_url)
                try:
                    video_lst = get_page.json()
                except:
                    continue
                else:
                    for video_dic in video_lst:
                        video_id = video_dic['codeId']
                        url = 'http://video.tudou.com/v/%s.html' % video_id
                        url_lst.append(url)
        print('push %s video url into redis set' % len(url_lst))
        connect_with_redis.push_video_url_to_redis_set(platform=self.platform, url_lst=url_lst)

    async def download_html(self, session, url):
        get_page = await session.get(url)
        page = await get_page.text("utf-8", errors="ignore")
        return url + "fangyuchenggoalkeeper" + page

    async def get_video_page(self, loop, task_lst):
        async with aiohttp.ClientSession() as session:
            video_page = [loop.create_task(self.download_html(session, url))
                          for url in task_lst]
            done, pending = await asyncio.wait(video_page)
            download_video_page_lst = [d.result() for d in done]
            connect_with_redis.push_video_page_html_to_redis(platform=self.platform,
                                                             result_lst=download_video_page_lst)

    def download_video_page_async(self):
        task_lst = connect_with_redis.retrieve_video_url_from_redis_set(platform=self.platform)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.get_video_page(loop, task_lst))

    def download_video_page_async_single_process(self):
        key = 'new_tudou_video_url'
        pid = os.getpid()
        while connect_with_redis.length_of_set(key) > 0:
            self.download_video_page_async()
            print("platform: %s, action: download video page, pid: %s"
                  % (self.platform, pid))

    def download_video_page_async_multi_process(self, process_num=10):
        pool = Pool(processes=process_num)
        for line in range(process_num):
            pool.apply_async(self.download_video_page_async_single_process)
        pool.close()
        pool.join()

    def parse_video_page_html(self, html):
        page_lst = html.split('fangyuchenggoalkeeper')
        url = page_lst[0]
        page = page_lst[1]
        soup = BeautifulSoup(page, 'html.parser')
        try:
            title = soup.find('h1', {'class': 'td-playbase__title'}).span.text
        except:
            title = None
        try:
            releaser = soup.find('a',{'class':'td-play__userinfo__name'}).text
        except:
            releaser = None
        try:
            midsteptime = soup.find('div',{'class':
                                           'td-play__videoinfo__details-box__time'}).text[:-2]
            release_time = int(datetime.datetime.strptime(midsteptime,
                                                          '%Y-%m-%d %H:%M:%S').timestamp()*1e3)
        except:
            release_time = None
        try:
            releaserUrl = soup.find("a", {"class": "td-play__userinfo__name"})['href']
        except:
            releaserUrl = None
        try:
            find_play_count = ' '.join(re.findall('total_vv.*stripe_bottom', page))
            replace_comma = find_play_count.replace(',', '')
            play_count_str = ' '.join(re.findall('total_vv":"\d+', replace_comma))
            play_count = int(' '.join(re.findall('\d+', play_count_str)))
        except:
            play_count = 0
        try:
            find_dura = re.findall('stripe_bottom":"\d+:\d+', page)
            dura_str = ' '.join(find_dura).split('":"')[-1]
            duration = trans_duration(dura_str)
        except:
            duration = 0
        fetch_time = int(time.time()*1e3)
        info_dic = {'platform': self.platform,
                    "title": title,
                    'url': url,
                    'duration': duration,
                    "releaser": releaser,
                    "release_time": release_time,
                    "releaserUrl": releaserUrl,
                    'play_count': play_count,
                    'fetch_time': fetch_time}
        return info_dic

    def parse_video_page_single_process(self,
                                        output_to_file=False,
                                        filepath=None,
                                        push_to_redis=False,
                                        output_to_es_raw=False,
                                        es_index=None,
                                        doc_type=None,
                                        output_to_es_register=False):
        error_url_list = []
        pid = os.getpid()
        key = 'new_tudou_video_page_html'
        result_lst = []
        while connect_with_redis.length_of_lst(key) > 0:
            video_page_html = connect_with_redis.retrieve_video_page_html_from_redis(platform=self.platform)
            video_info = self.parse_video_page_html(video_page_html)
            try:
                video_info['title']
            except:
                continue
            if video_info['title'] is not None:
                result_lst.append(video_info)
                print("platform: %s, action: parse video page, process_id: %s,"
                      "count number: %s" % (self.platform, pid, len(result_lst)))
                if len(result_lst) >= 1000:
                    output_result(result_Lst=result_lst,
                                  platform=self.platform,
                                  output_to_file=output_to_file,
                                  filepath=filepath,
                                  push_to_redis=push_to_redis,
                                  output_to_es_raw=output_to_es_raw,
                                  es_index=es_index,
                                  doc_type=doc_type,
                                  output_to_es_register=output_to_es_register)
                    result_lst.clear()
            else:
                try:
                    error_url_list.append(video_info['url'])
                except:
                    pass
        if result_lst != []:
            output_result(result_Lst=result_lst,
                          platform=self.platform,
                          output_to_file=output_to_file,
                          filepath=filepath,
                          push_to_redis=push_to_redis,
                          output_to_es_raw=output_to_es_raw,
                          es_index=es_index,
                          doc_type=doc_type,
                          output_to_es_register=output_to_es_register)
        if error_url_list != []:
            connect_with_redis.push_video_url_to_redis_set(platform=self.platform,
                                                           url_lst=error_url_list)

    def parse_video_page_multi_process(self,
                                       para_dict,
                                       process_num=30):
        pool = Pool(processes=process_num)
        for line in range(process_num):
            pool.apply_async(self.parse_video_page_single_process, kwds=para_dict)
        pool.close()
        pool.join()

    def key_customer(self, releaserUrl,
                     releaser_page_num_max=1000,
                     output_to_es_raw=False,
                     es_index='crawler-data-raw',
                     doc_type='doc'):
        """
        input releaserUrl must be strict as https://id.tudou.com/i/UMjc5MzI5NDA==/videos?
        end with /videos otherwise when scrolling it will make mistakes
        """
        releaser_id = self.get_releaser_id(releaserUrl)
        print("working on releaser: %s" % releaser_id)
        releaserUrl = 'https://id.tudou.com/i/%s/videos' % releaser_id
        result_lst = []
        get_page = retry_get_url(releaserUrl)
        get_page.encoding = 'utf-8'
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        try:
            releaser = soup.find('div', {'class': 'user-name'}).a.text
        except:
            releaser = None
        try:
            total_video_num_str = soup.find('div', {'class': 'title'}).span.text
            total_video_num = total_video_num_str.replace('(', '').replace(')', '').replace(',', '')
            total_video_num = trans_play_count(total_video_num)
        except:
            print(releaserUrl)
        if total_video_num % 50 == 0:
            total_page_num = int(total_video_num/50)
        else:
            total_page_num = int(total_video_num/50) + 1
        if releaser_page_num_max > total_page_num:
            releaser_page_num_max = total_page_num
        print("releaser page num max is %s" % releaser_page_num_max)
        video_lst = soup.find_all('div', {'class': 'v'})
        for line in video_lst:
            video_info = self.process_one_video(line)
            video_info['releaserUrl'] = releaserUrl
            video_info['releaser'] = releaser
            result_lst.append(video_info)
        if releaser_page_num_max >= 2:
            page_num = 2
            try:
                partial_releaserUrl = soup.find('li',{'class':'next'}).a['href']
                new_releaserUrl = 'https://id.tudou.com%s' % partial_releaserUrl
            except:
                print(new_releaserUrl)
            while page_num <= releaser_page_num_max:
                get_page = retry_get_url(new_releaserUrl)
                get_page.encoding = 'utf-8'
                page = get_page.text
                soup = BeautifulSoup(page, 'html.parser')
                if page_num != releaser_page_num_max:
                    try:
                        new_releaserUrl = 'https://id.tudou.com' + soup.find('li',{'class':'next'}).a['href']
                    except:
                        new_releaserUrl = ('https://id.tudou.com/i/%s/videos?order=1&page=%s' % (releaser_id, page_num))
                video_lst = soup.find_all('div', {'class': 'v'})
                for line in video_lst:
                    video_info = self.process_one_video(line)
                    video_info['releaserUrl'] = releaserUrl
                    video_info['releaser'] = releaser
                    result_lst.append(video_info)
                print('get page %s list length is %s' % (page_num, len(result_lst)))
                page_num += 1
                output_result(result_Lst=result_lst,
                              platform=self.platform,
                              output_to_es_raw=output_to_es_raw,
                              es_index=es_index,
                              doc_type=doc_type)
                result_lst.clear()
        if result_lst != []:
            output_result(result_Lst=result_lst,
                          platform=self.platform,
                          output_to_es_raw=output_to_es_raw,
                          es_index=es_index,
                          doc_type=doc_type)
            result_lst.clear()

    def get_releaser_image(self,releaserUrl=None,data=None):
        if releaserUrl:
            get_page = requests.get(releaserUrl)
            get_page.encoding = 'utf-8'
            page = get_page.text
            soup = BeautifulSoup(page, 'html.parser')
            try:
                image_url = soup.find('a', {'class': 'user-avatar'}).next_element["src"]
                print(image_url)
                return image_url
            except:
                print("can't get image")
        else:
            image_url = data.find('a', {'class': 'user-avatar'}).next_element["src"]
            print(image_url)
            return image_url

    @staticmethod
    def get_video_image(data):
        video_photo_url = data["cover"]["big"]["url"]
        return video_photo_url

    def get_releaser_name(self, releaserUrl):

        """
        Due to the function releaser_page can't get releaser name from api,
        I add a function to get it from web page
        posted by yucheng fang
        """
        get_page = requests.get(releaserUrl,proxies=get_proxy(1))
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        try:
            releaser = soup.find('div', {'class': 'user-name'}).a.text
        except:
            print("can't get releaser name at soup.find('div', {'class': 'user-name'}).a.text")
        if releaser is not None:
            print("get releaser name at soup.find('div', {'class': 'user-name'}).a.text")
            return releaser
        else:
            print("get releaser name at soup.find('div', {'class': 'user-name'}).a.text")
        try:
            releaser = soup.find('div', {'class': 'head-avatar'}).a['title']
        except:
            print("can't get releaser name at soup.find('div', {'class': 'head-avatar'}).a['title']")
        if releaser is not None:
            return releaser
        else:
            print("can't get releaser name at soup.find('div', {'class': 'head-avatar'}).a['title']")
        return None

    def rebuild_releaserUrl(self, releaserUrl):
        get_page = requests.get(releaserUrl)
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        releaser_id = soup.find('div', {'class': 'user-name'}).a['href']
        url = 'https://id.tudou.com' + releaser_id + '/videos'
        return url

    def releaser_page(self,releaserUrl,**kwargs):
        for res in self.releaser_page_web(releaserUrl,**kwargs):
            yield res

#    @logged
    def releaser_page_app(self, releaserUrl,
                      output_to_file=False,
                      filepath=None,
                      releaser_page_num_max=4000,
                      output_to_es_raw=False,
                      es_index=None,
                      doc_type=None,
                      output_to_es_register=False,
                      push_to_redis=False,
                      proxies_num=None
                      ):

        """
        get video info from api instead of web page html
        the most scroll page is 1000
        """

        headers = {'Host': 'apis.tudou.com',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                   'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                   'Accept-Encoding': 'gzip, deflate',
                   'Connection': 'keep-alive',
                   'Cookie': ('isg=BIeH6gcJlwZw_xQESm9jlG-vFTuRJGXxikf0g1l0mJY9yKeKYVuAvzKJbkgzOzPm;'
                              'cna=XA2EFIGslWoCAWp4y3KXcZh7; ykss=cdbd115c102a68710215ad93;'
                              '__ysuid=1543316262167mjE; P_ck_ctl=62DE1D55DFE1C0F4F27A8662E6575F08;'
                              '__ayvstp=32'),
                   'Upgrade-Insecure-Requests': '1',
                   'Cache-Control': 'max-age=0'}

        count = 1
#        has_more = True
        retry_time = 0
        result_list = []
        releaser_id = self.get_releaser_id(releaserUrl)
        releaser = self.get_releaser_name(releaserUrl)
        releaserUrl = 'https://id.tudou.com/i/%s/videos' % releaser_id
        self.video_data['releaser'] = releaser
        self.video_data['releaserUrl'] = releaserUrl
        url_dic = {"uid": releaser_id,
                   "pL": "20"}
        print("working on releaser: %s releaser_id: %s" % (releaser, releaser_id))
        while count <= releaser_page_num_max and retry_time < 5:
            proxies = get_proxy(proxies_num)
            url_dic['pg'] = str(count)
            url_dic['pn'] = str(count)
            api_url = 'http://apis.tudou.com/subscribe/v1/video?%s' % urllib.parse.urlencode(url_dic)
            # print(api_url)
            if proxies:
                get_page = requests.get(api_url, headers=headers,proxies=proxies,timeout=5)
            else:
                get_page = requests.get(api_url, headers=headers,timeout=5)
            page_dic = get_page.json()
#            has_more = page_dic.get('has_more')
            try:
                data_list = page_dic['entity']
            except:
                retry_time += 1
                proxies = get_proxy(1)
                time.sleep(0.25)
                print("no more data at releaser: %s page: %s try_time: %s" % (releaser, count, retry_time))
                continue
            if data_list == []:
                retry_time += 1
                proxies = get_proxy(1)
                time.sleep(0.25)
                print("no more data at releaser: %s page: %s try_time: %s" % (releaser, count, retry_time))
                continue
            else:
                retry_time = 0
                print("get data at releaser: %s page: %s" % (releaser, count))
                count += 1
                for info_dic in data_list:
                    video_info = copy.deepcopy(self.video_data)
                    one_video = info_dic.get('detail')
                    if one_video is not None:
                        get_title = one_video.get('base_detail')
                        if get_title is not None:
                            video_info['title'] = get_title.get('title')
                        detail_info = one_video.get('video_detail')
                        if detail_info is not None:
                            video_id = detail_info.get('video_id')
                            if video_id is not None:
                                video_info['video_id'] = video_id
                                video_info['url'] = 'https://video.tudou.com/v/%s.html' % video_id
                            video_info['duration'] = detail_info.get('duration')
                            video_info['releaser_id_str'] = "new_tudou_%s" % (releaser_id)
                            video_info['comment_count'] = int(detail_info.get('comment_count'))
                            video_info['favorite_count'] = int(detail_info.get('praiseNumber'))
                            #favorite_count in database means 点赞数, while in web page the factor
                            #named praiseNumber
                            #in web page facorite_count means 收藏数
                            video_info['shoucang_count'] = (detail_info.get('favorite_count'))
                            # print('play_count', detail_info.get('vv_count'))
                            video_info['play_count'] = detail_info.get('vv_count')
                            video_info['video_img'] = self.get_video_image(detail_info)
                            release_time_str = detail_info.get('publish_time')
                            print(release_time_str)
#                            print(video_info['release_time'])
                            if '天前' in release_time_str:
                                video_info['release_time'] = self.video_page(video_info['url'])['release_time']
                            else:
                                video_info['release_time'] = trans_strtime_to_timestamp(input_time=release_time_str,
                                                                                        missing_year=True)
                            video_info['fetch_time'] = int(time.time() * 1e3)
                            yield video_info
                    else:
                        continue

    def releaser_page_by_time(self, start_time, end_time, url,allow,**kwargs):
        count_false = 0
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
# test
if __name__=='__main__':
    test = Crawler_tudou()
    # url = 'https://video.tudou.com/v/XNDExNjcyNTI0MA==.html'
    releaser_url = "https://id.tudou.com/i/UNTUzMTU1ODg2OA=="
    # ttt = test.video_page("https://video.tudou.com/v/XNDExNjcyNTI0MA==.html")
    #releaserUrl=url, output_to_es_raw=True,
    #                           es_index='crawler-data-raw',
    #                           doc_type='doc',
    #                           releaser_page_num_max=100)
    test.releaser_page_by_time(1569081600000,1570610953322 ,releaser_url,output_to_es_raw=True,es_index='crawler-data-raw',
doc_type='doc',releaser_page_num_max=4000)
#     test.get_releaser_image(releaser_url)
#     test.get_releaser_follower_num()
