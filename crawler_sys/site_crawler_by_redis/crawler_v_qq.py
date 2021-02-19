# -*- coding: utf-8 -*-
"""
Created on Sat Feb 24 14:19:00 2018

independent function:
1 list page
2 search page
3 video page
4 releaser page

All function take parameters and return dict list.

parameters:
1 list page: url
2 search page: keyword
3 video page: url
4 releaser page: url


@author: fangyucheng
"""

import os
import asyncio
import copy
import requests
import re
import datetime
import json
import aiohttp
import random
from bs4 import BeautifulSoup
from multiprocessing import Pool
# from multiprocessing import Process
from crawler.crawler_sys.framework.video_fields_std import Std_fields_video
from crawler.crawler_sys.utils.output_results import retry_get_url
from crawler.crawler_sys.utils.output_results import output_result
# from crawler.crawler_sys.utils import output_log
from crawler.crawler_sys.utils.trans_str_play_count_to_int import trans_play_count
from crawler.crawler_sys.utils import connect_with_redis
from crawler.crawler_sys.utils.trans_duration_str_to_second import trans_duration
from crawler.crawler_sys.utils.util_logging import logged
import urllib
from crawler.crawler_sys.proxy_pool.func_get_proxy_form_kuaidaili import get_proxy
try:
    from crawler_sys.framework.func_get_releaser_id import *
except:
    from func_get_releaser_id import *


class Crawler_v_qq():
    def __init__(self, timeout=None, platform='腾讯视频'):
        if timeout == None:
            self.timeout = 10
        else:
            self.timeout = timeout
        self.platform = platform
        std_fields = Std_fields_video()
        self.video_data = std_fields.video_data
        self.video_data['platform'] = self.platform
        # remove fields that crawled data don't have
        pop_key_Lst = ['describe', 'repost_count', 'isOriginal',
                       'video_id']
        for popk in pop_key_Lst:
            self.video_data.pop(popk)

        self.list_page_url_dict = {
            '电影': 'http://v.qq.com/x/list/movie',
            '电视剧': 'http://v.qq.com/x/list/tv',
            '综艺': 'http://v.qq.com/x/list/variety',
            '动漫': 'http://v.qq.com/x/list/cartoon',
            '少儿': 'http://v.qq.com/x/list/children',
            '音乐': 'http://v.qq.com/x/list/music',
            '纪录片': 'http://v.qq.com/x/list/doco',
            '新闻': 'http://v.qq.com/x/list/news',
            '军事': 'http://v.qq.com/x/list/military',
            '娱乐': 'http://v.qq.com/x/list/ent',
            '体育': 'http://v.qq.com/x/list/sports',
            '游戏': 'http://v.qq.com/x/list/games',
            '搞笑': 'http://v.qq.com/x/list/fun',
            '微电影': 'http://v.qq.com/x/list/dv',
            '时尚': 'http://v.qq.com/x/list/fashion',
            '生活': 'http://v.qq.com/x/list/life',
            '母婴': 'http://v.qq.com/x/list/baby',
            '汽车': 'http://v.qq.com/x/list/auto',
            '科技': 'http://v.qq.com/x/list/tech',
            '教育': 'http://v.qq.com/x/list/education',
            '财经': 'http://v.qq.com/x/list/finance',
            '房产': 'http://v.qq.com/x/list/house',
            '旅游': 'http://v.qq.com/x/list/travel',
        }
        self.legal_list_page_urls = []
        self.legal_channels = []
        for ch in self.list_page_url_dict:
            list_page_url = self.list_page_url_dict[ch]
            self.legal_list_page_urls.append(list_page_url)
            self.legal_channels.append(ch)

    #        log_path = '/home/hanye/crawlersNew/crawler/crawler_log'
    #        current_day = str(datetime.datetime.now())[:10]
    #        info_log_file = log_path + '/all_' + current_day + '.log'
    #        info_name = self.platform + '_info'
    #        self.loggerinfo = output_log.init_logger(name=info_name, log_file=info_log_file)

    def video_page(self, url, channel=None):
        if 'm.v.qq.com' in url:
            vid_str = ' '.join(re.findall('o/d/y/.*.html', url))
            vid = vid_str.replace('o/d/y/', '').replace('.html', '')
            url = 'https://v.qq.com/x/page/' + vid + '.html'
        get_page = retry_get_url(url, timeout=self.timeout)
        if get_page is None:
            return None
        else:
            get_page.encoding = 'utf-8'
            page = get_page.text
            soup = BeautifulSoup(page, 'html.parser')
            try:
                title = soup.find('h1', {'class': 'video_title _video_title'}).text
                title = title.replace('\n', '')
                title = title.replace('\t', '')
                # remove leading and trailing spaces
                title = re.sub('(^\s+)|(\s+$)', '', title)
            except:
                try:
                    title = soup.find('h1', {'class': 'video_title'}).text
                    title = title.replace('\n', '')
                    title = title.replace('\t', '')
                    title = re.sub('(^\s+)|(\s+$)', '', title)
                except:
                    title = None
            try:
                releaser = soup.find('span', {'class': 'user_name'}).text
            except:
                releaser = None
            try:
                releaserUrl = soup.find('a', {'class': 'user_info'})['href']
            except:
                releaserUrl = None
            try:
                video_intro = soup.find('meta', {'itemprop': 'description'})['content']
            except:
                video_intro = None
            soup_find = soup.find("script", {"r-notemplate": "true"})
            if soup_find != None:
                midstep = soup_find.text
            else:
                print('Failed to get correct html text with soup')
                return None
            video_info_var_Lst = re.findall('var\s+VIDEO_INFO\s+=\s*{.+}', midstep)
            if video_info_var_Lst != []:
                video_info_var = video_info_var_Lst[0]
                video_info_json = re.sub('var\s+VIDEO_INFO\s+=\s*', '', video_info_var)
                try:
                    video_info_dict = json.loads(video_info_json)
                except:
                    print('Failed to transfer video_info_json to dict '
                          'for url: ' % url)
                    video_info_dict = {}
                if video_info_dict != {}:
                    if 'duration' in video_info_dict:
                        duration_str = video_info_dict['duration']
                        duration = int(duration_str)
                    else:
                        duration = None
                    if 'title' in video_info_dict:
                        title = video_info_dict['title']
                    if 'view_all_count' in video_info_dict:
                        play_count = video_info_dict['view_all_count']
                        data_source = 'video_info'
                    else:
                        try:
                            play_count_str = re.findall('interactionCount.*', page)[0]
                            play_count = re.findall('\d+', play_count_str)[0]
                            play_count = int(play_count)
                            data_source = 'interactioncount'
                        except:
                            play_count = None
                    if 'video_checkup_time' in video_info_dict:
                        release_time_str = video_info_dict['video_checkup_time']
                        try:
                            release_time_ts = int(datetime.datetime.strptime(
                                release_time_str, '%Y-%m-%d %H:%M:%S'
                            ).timestamp() * 1e3)
                        except:
                            release_time_ts = None
                    else:
                        release_time_ts = None
            else:
                try:
                    play_count_str = re.findall('interactionCount.*', page)[0]
                    play_count = re.findall('\d+', play_count_str)[0]
                    play_count = int(play_count)
                    data_source = 'interactioncount'
                except:
                    play_count = None
                try:
                    release_time_str = soup.find('span', {'class': 'tag_item'}).text
                    re_lst = re.findall('\d+', release_time_str)
                    release_time_raw = re_lst[0] + '-' + re_lst[1] + '-' + re_lst[2]
                    release_time_ts = int(datetime.datetime.strptime(release_time_raw, '%Y-%m-%d').timestamp() * 1e3)
                except:
                    release_time_ts = None
                try:
                    duration_str = re.findall('duration.*', page)[0]
                    duration = int(re.findall('\d+', duration_str)[0])
                except:
                    duration = None
            fetch_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1e3)
            video_dict = copy.deepcopy(self.video_data)
            if play_count != None:
                video_dict['title'] = title
                video_dict['data_source'] = data_source
                if channel is not None:
                    video_dict['channel'] = channel
                video_dict['releaser'] = releaser
                video_dict['play_count'] = play_count
                video_dict['release_time'] = release_time_ts
                video_dict['duration'] = duration
                video_dict['url'] = url
                video_dict['fetch_time'] = fetch_time
                if releaserUrl is not None:
                    video_dict['releaserUrl'] = releaserUrl
                if video_intro is not None:
                    video_dict['video_intro'] = video_intro
            return video_dict

    def search_page(self, keyword, search_pages_max=30,
                    output_to_es_raw=False,
                    output_to_es_register=False,
                    es_index=None,
                    doc_type=None):
        search_page_Lst = []

        def process_one_line(data_line):
            url = data_line.h2.a['href']
            dicdicdic = self.video_page(url)
            return dicdicdic

        search_url = ('https://v.qq.com/x/search?q='
                      + keyword
                      + '&cur={}'.format(str(i)) for i in range(1, search_pages_max + 1))
        for urls in search_url:
            get_page = requests.get(urls, timeout=self.timeout)
            print(urls)
            get_page.encoding = 'utf-8'
            page = get_page.text
            soup = BeautifulSoup(page, 'html.parser')
            tencent = soup.find_all("div", {"class": "result_item result_item_h _quickopen"})
            for data_line in tencent:
                one_line_dic = process_one_line(data_line)
                print('get one line done')
                print(one_line_dic)
                search_page_Lst.append(one_line_dic)

            if len(search_page_Lst) >= 100:
                output_result(result_Lst=search_page_Lst,
                              platform=self.platform,
                              output_to_es_raw=output_to_es_raw,
                              output_to_es_register=output_to_es_register,
                              es_index=es_index,
                              doc_type=doc_type)
                search_page_Lst.clear()

        if search_page_Lst != []:
            output_result(result_Lst=search_page_Lst,
                          platform=self.platform,
                          output_to_es_raw=output_to_es_raw,
                          output_to_es_register=output_to_es_register,
                          es_index=es_index,
                          doc_type=doc_type)
        return search_page_Lst

    # list page synchronous
    def list_page_sync(self, listurl, channel=None,
                       output_to_file=False, filepath=None,
                       output_to_es_raw=False,
                       output_to_es_register=False,
                       push_to_redis=False,
                       page_num_max=34,
                       output_es_index=None,
                       output_doc_type=None,
                       ):
        if channel is None:
            channel = listurl.split('list/')[-1]
        # listurl=http://v.qq.com/x/list/fashion/
        list_data_Lst = []
        listnum = []
        videos_in_one_page = 30
        for i in range(0, page_num_max):
            list_num = i * videos_in_one_page
            listnum.append(list_num)
        # 最近热播
        listpage = [listurl + '/?sort=40&offset={}'.format(str(i)) for i in listnum]
        # 最近上架
        # listpage=[listurl+'?sort=5&offset={}'.format(str(i)) for i in listnum]
        for listurls in listpage:
            get_page = retry_get_url(listurls, timeout=self.timeout)
            if get_page is None:
                print('Failed to get page for list page url: %s'
                      % listurls)
                return None
            get_page.encoding = 'utf-8'
            page = get_page.text
            print(listurls)
            soup = BeautifulSoup(page, 'html.parser')
            midstep = soup.find_all('li', {'class': 'list_item'})
            for line in midstep:
                one_video_dic = {}
                url = line.a['href']
                try:
                    one_video_dic = self.video_page(url, channel)
                    find_play_count = BeautifulSoup(list(line)[-2], 'html.parser')
                    play_count_str1 = find_play_count.find('span', {'class': 'num'}).text
                    play_count_str2 = play_count_str1.replace(' ', '')
                    try:
                        play_count = trans_play_count(play_count_str2)
                    except:
                        play_count = 0
                    one_video_dic['play_count'] = play_count
                    list_data_Lst.append(one_video_dic)
                    if len(list_data_Lst) >= 100:
                        if output_es_index != None and output_doc_type != None:
                            output_result(list_data_Lst,
                                          self.platform,
                                          output_to_file=output_to_file,
                                          filepath=filepath,
                                          output_to_es_raw=output_to_es_raw,
                                          output_to_es_register=output_to_es_register,
                                          push_to_redis=push_to_redis,
                                          es_index=output_es_index,
                                          doc_type=output_doc_type)
                            list_data_Lst.clear()
                        else:
                            output_result(list_data_Lst,
                                          self.platform,
                                          output_to_file=output_to_file,
                                          filepath=filepath,
                                          output_to_es_raw=output_to_es_raw,
                                          output_to_es_register=output_to_es_register,
                                          push_to_redis=push_to_redis)
                            list_data_Lst.clear()
                except:
                    print('failed to get data from %s' % url)
        if list_data_Lst != []:
            if output_es_index != None and output_doc_type != None:
                output_result(list_data_Lst,
                              self.platform,
                              output_to_file=output_to_file,
                              filepath=filepath,
                              output_to_es_raw=output_to_es_raw,
                              output_to_es_register=output_to_es_register,
                              push_to_redis=push_to_redis,
                              es_index=output_es_index,
                              doc_type=output_doc_type)
                list_data_Lst.clear()
            else:
                output_result(list_data_Lst,
                              self.platform,
                              output_to_file=output_to_file,
                              filepath=filepath,
                              output_to_es_raw=output_to_es_raw,
                              output_to_es_register=output_to_es_register,
                              push_to_redis=push_to_redis)
                list_data_Lst.clear()
        return list_data_Lst


    def get_releaser_image(self,releaserUrl=None,data=None):
        if releaserUrl:
            get_page = requests.get(releaserUrl)
            get_page.encoding = 'utf-8'
            page = get_page.text
            try:
                image_url = re.findall('class="top_avatar_pic"\n          src="(.*)"', page)[0]
                # follower_str = soup.find('span', {'class': 'value _follow_number'}).text
                print(image_url)
                return "http:" + image_url
            except:
                print("can't get image_url")
        else:
            image_url = re.findall('class="top_avatar_pic"\n          src="(.*)"', data)[0]
            # follower_str = soup.find('span', {'class': 'value _follow_number'}).text
            print(image_url)
            return "http:" + image_url

    @staticmethod
    def get_video_image(data):
        video_photo_url = data["pic_496x280"]
        return video_photo_url

    def doc_list_page(self, listurl):
        # listurl=http://v.qq.com/x/list/fashion/
        done = open('done_qq', 'a')
        result = open('result_qq', 'a')
        error = open('error_qq', 'a')
        list_data_Lst = []
        listnum = []
        for i in range(0, 93):
            list_num = i * 30
            listnum.append(list_num)
        # 最近热播
        listpage = [listurl + '?&offset={}'.format(str(i)) for i in listnum]
        # 最近上架
        # listpage=[listurl+'?sort=5&offset={}'.format(str(i)) for i in listnum]
        for listurl in listpage:
            get_page = requests.get(listurl, timeout=self.timeout)
            get_page.encoding = 'utf-8'
            page = get_page.text
            print(listurl)
            done.write(listurl)
            done.write('\n')
            done.flush()
            soup = BeautifulSoup(page, 'html.parser')
            midstep = soup.find_all('strong', {'class': 'figure_title'})
            for line in midstep:
                album_name = line.text
                url = line.a['href']
                get_page = requests.get(url, timeout=self.timeout)
                get_page.encoding = 'utf-8'
                page = get_page.text
                soup = BeautifulSoup(page, 'html.parser')
                try:
                    get_all_url = soup.find('ul', {'class': 'figure_list _hot_wrapper'})
                    url_agg = get_all_url.find_all('a', {'class': 'figure_detail'})
                    urllist = []
                    for line in url_agg:
                        url_part = line['href']
                        url = 'https://v.qq.com' + url_part
                        urllist.append(url)
                    for url in urllist:
                        try:
                            one_video = self.video_page(url)
                            one_video['album_name'] = album_name
                            print(url)
                            list_data_Lst.append(one_video)
                            one_video_json = json.dumps(one_video)
                            result.write(one_video_json)
                            result.write('\n')
                            result.flush()
                        except AttributeError:
                            D0 = {'url': url, 'album_name': album_name}
                            print('there is an error')
                            json_D0 = json.dumps(D0)
                            error.write(json_D0)
                            error.write('\n')
                            error.flush()
                except:
                    one_video = self.video_page(url)
                    one_video['album_name'] = album_name
                    print(url)
                    list_data_Lst.append(one_video)
                    one_video_json = json.dumps(one_video)
                    result.write(one_video_json)
                    result.write('\n')
                    result.flush()
        done.close()
        result.close()
        error.close()
        return list_data_Lst

    def get_releaser_id(self, releaserUrl):
        return get_releaser_id(platform=self.platform, releaserUrl=releaserUrl,is_qq=True)


    def get_release_time_from_str(self, rt_str):
        minute = '分钟'
        hour = '小时'
        day = '天'
        if minute in rt_str:
            rt_int = int(re.findall('\d+', rt_str)[0])
            rt = datetime.datetime.timestamp(datetime.datetime.now()) - rt_int * 60
            release_time = int(rt * 1e3)
        elif hour in rt_str:
            rt_int = int(re.findall('\d+', rt_str)[0])
            rt = datetime.datetime.timestamp(datetime.datetime.now()) - rt_int * 3600
            release_time = int(rt * 1e3)
        elif day in rt_str:
            rt_int = int(re.findall('\d+', rt_str)[0])
            rt = datetime.datetime.timestamp(datetime.datetime.now()) - rt_int * 3600 * 60
            release_time = int(rt * 1e3)
        else:
            release_time = int(datetime.datetime.strptime(rt_str, '%Y-%m-%d').timestamp() * 1e3)
        return release_time

    # @logged
    def releaser_page(self, releaserUrl,
                      output_to_file=False, filepath=None,
                      output_to_es_raw=False,
                      output_to_es_register=False,
                      push_to_redis=False,
                      releaser_page_num_max=10000,
                      es_index=None,
                      doc_type=None,proxies_num=None):
        print('Processing releaserUrl %s' % releaserUrl)
        result_Lst = []
        releaser_info = self.get_releaser_id(releaserUrl)
        number_id = releaser_info['number_id']
        releaser_id = releaser_info['releaser_id']
        releaser = releaser_info['releaser']
        pagenum = 0
        has_more = True
        if releaser_id != None:
            while pagenum <= releaser_page_num_max and has_more:
                pagenum += 1
                url_dic = {
                    "vappid": "50662744",
                    "vsecret": "64b037e091deae75d3840dbc5d565c58abe9ea733743bbaf",
                    "iSortType": "0",
                    "page_index": pagenum,
                    "hasMore": "true",
                    "stUserId": number_id,
                    "page_size": "20",
                    "_": datetime.datetime.now().timestamp()
                }
                releaser_page_url = ('http://access.video.qq.com/pc_client/GetUserVidListPage?%s' % urllib.parse.urlencode(url_dic))
                print('Page number: %d'% pagenum)
                try:
                    if proxies_num:
                        get_page = retry_get_url(releaser_page_url, timeout=self.timeout,proxies=proxies_num)
                    else:
                        get_page = retry_get_url(releaser_page_url,timeout=self.timeout)
                except:
                    get_page = None
                    has_more = False
                if get_page != None and get_page.status_code == 200:
                    get_page.encoding = 'utf-8'
                    page = get_page.text
                    real_page = page[5:]
                    real_page = real_page.replace('null', 'None')
                    try:
                        get_page_dic = eval(real_page)
                        page_dic = get_page_dic["data"]['vecVidInfo']
                    except:
                        page_dic = None
                        has_more = False
                    if page_dic != None:
                        for a_video in page_dic:
                            try:
                                video_dic = copy.deepcopy(self.video_data)
                                vid = a_video.get("vid")
                                video_info = a_video.get("mapKeyValue")
                                title = video_info['title']
                                try:
                                    play_count = int(float(video_info['view_all_count']))
                                except:
                                    play_count = 0
                                rt_str = video_info['create_time']
                                release_time = datetime.datetime.strptime(rt_str,"%Y-%m-%d %H:%M")
                                url = "https://v.qq.com/x/page/%s.html" % vid
                                duration = int(video_info['duration'])
                                fetch_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1e3)
                                video_dic['title'] = title
                                video_dic['duration'] = duration
                                video_dic['url'] = url
                                video_dic['play_count'] = play_count
                                if play_count <= 300:
                                    video_dic['comment_count'] = 0
                                else:
                                    video_dic['comment_count'] = self.func_get_comment_count(url)
                                video_dic['releaser'] = releaser
                                video_dic['release_time'] = int(release_time.timestamp()*1e3)
                                video_dic['fetch_time'] = fetch_time
                                video_dic["video_id"] = vid
                                video_dic["releaser_id_str"] = "腾讯视频_%s"%(releaser_id)
                                video_dic['releaserUrl'] = "http://v.qq.com/vplus/%s" % releaser_id
                                video_dic['video_img'] = self.get_video_image(video_info)
                                yield video_dic
                            except Exception as e:
                                print(e)
                                continue

    def get_releaser_follower_num(self, releaserUrl):
        proxies = get_proxy(1)
        if proxies:
            get_page = requests.get(releaserUrl, proxies=proxies,timeout=3)
        else:
            get_page = requests.get(releaserUrl,timeout=3)
        get_page.encoding = 'utf-8'
        page = get_page.text

        # soup = BeautifulSoup(page, 'html.parser')
        try:
            follower_str = re.findall('data-number="(\d*)"', page)[0]
            # follower_str = soup.find('span', {'class': 'value _follow_number'}).text
            follower_num = trans_play_count(follower_str)
            print('%s follower number is %s' % (releaserUrl, follower_num))
            releaser_img_url = self.get_releaser_image(data=page)
            return follower_num, releaser_img_url
        except:
            print("can't can followers")

    # list page asynchronous which is main stream in the future
    def start_list_page(self, task_list):
        self.list_page_task(task_list)
        self.run_list_page_asyncio()

    def list_page_task(self, task_list,
                       page_num_max=34):
        lst_page_task_lst = []
        for list_url in task_list:
            videos_in_one_page = 30
            num_lst = []
            for i in range(0, page_num_max):
                num = i * videos_in_one_page
                num_lst.append(num)
            task_url_lst_new = [list_url + '/?sort=5&offset=' + str(num) for num in num_lst]
            lst_page_task_lst.extend(task_url_lst_new)
            task_url_lst_hot = [list_url + '/?sort=40&offset=' + str(num) for num in num_lst]
            lst_page_task_lst.extend(task_url_lst_hot)
            task_url_lst_praise = [list_url + '/?sort=48&offset=' + str(num) for num in num_lst]
            lst_page_task_lst.extend(task_url_lst_praise)
        random.shuffle(lst_page_task_lst)
        connect_with_redis.push_list_url_to_redis(platform=self.platform, result_lst=lst_page_task_lst)

    async def download_page(self, session, url):
        get_page = await session.get(url)
        page = await get_page.text("utf-8", errors="ignore")
        return page

    async def list_page(self, loop, task_lst):
        async with aiohttp.ClientSession() as sess_lst_page:
            task_video_page = [loop.create_task(self.download_page(sess_lst_page,
                                                                   lst_url)) for lst_url in task_lst]
            lst_result, unfinished = await asyncio.wait(task_video_page)
            lst_page_html_lst = [v.result() for v in lst_result]
            connect_with_redis.push_list_page_html_to_redis(platform=self.platform,
                                                            result_lst=lst_page_html_lst)
            print("the length of url list is %s" % len(lst_page_html_lst))

    def run_list_page_asyncio(self):
        """
        get list page asynchronously
        """
        key = 'v_qq_list_url'
        while connect_with_redis.length_of_lst(key) > 0:
            task_lst = connect_with_redis.retrieve_list_url_from_redis(platform=self.platform,
                                                                       retrieve_count=20)
            print('the length of task list is %s' % len(task_lst))
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.list_page(loop, task_lst=task_lst))

    def parse_list_page_single_process(self):
        key = 'v_qq_list_page_html'
        while connect_with_redis.length_of_lst(key) > 0:
            lst_page_html = connect_with_redis.retrieve_list_page_html_from_redis(platform=self.platform)
            url_lst = self.process_list_page(resp=lst_page_html)
            connect_with_redis.push_url_dict_lst_to_redis_set(platform=self.platform,
                                                              result_lst=url_lst)
            print('push %s url dicts into redis' % len(url_lst))

    def parse_list_page_multi_process(self, process_num=20):
        pool = Pool(processes=process_num)
        for line in range(process_num):
            pool.apply_async(func=self.parse_list_page_single_process)
        pool.close()
        pool.join()
        # self.loggerinfo.info('finish parsing list page on %s' % str(datetime.datetime.now()))

    def process_list_page(self, resp):
        video_lst = []
        soup = BeautifulSoup(resp, 'html.parser')
        channel = soup.find('div', {'class': 'filter_list'}).find('a',
                                                                  {'class': 'filter_item current open'}).text
        midstep = soup.find_all('li', {'class': 'list_item'})
        for line in midstep:
            video_dic = {}
            url = line.a['href']
            find_play_count = BeautifulSoup(list(line)[-2], 'html.parser')
            play_count_str = find_play_count.find('span', {'class': 'num'}).text.replace(' ', '')
            try:
                play_count = trans_play_count(play_count_str)
            except:
                play_count = 0
            video_dic = {"url": url,
                         "play_count": play_count,
                         "channel": channel}
            video_lst.append(video_dic)
        return video_lst

    async def asynchronous_get_video_page(self, session, data_dic):
        channel = data_dic['channel']
        url = data_dic['url']
        play_count = data_dic['play_count']
        get_page = await session.get(url)
        page = await get_page.text("utf-8", errors="ignore")
        return (channel + 'fangyuchenggoalkeeper' + str(play_count) +
                'fangyuchenggoalkeeper' + url + 'fangyuchenggoalkeeper' + page)

    async def get_video_page(self, loop, task_lst):
        async with aiohttp.ClientSession() as sess_video_page:
            task_video_page = [loop.create_task(self.asynchronous_get_video_page(sess_video_page,
                                                                                 data_dic)) for data_dic in task_lst]
            video_result, unfinished = await asyncio.wait(task_video_page)
            video_page_download_result_lst = [v.result() for v in video_result]
            connect_with_redis.push_video_page_html_to_redis(platform=self.platform,
                                                             result_lst=video_page_download_result_lst)

    def process_video_page_helper(self, soup):
        try:
            title = soup.find('h1', {'class': 'video_title _video_title'}).text
            title = title.replace('\n', '')
            title = title.replace('\t', '')
            title = re.sub('(^\s+)|(\s+$)', '', title)
        except:
            try:
                title = soup.find('h1', {'class': 'video_title'}).text
                title = title.replace('\n', '')
                title = title.replace('\t', '')
                title = re.sub('(^\s+)|(\s+$)', '', title)
            except:
                title = None
        try:
            video_intro = soup.find('meta', {'itemprop': 'description'})['content']
        except:
            video_intro = None
        try:
            release_time_str = soup.find('span', {'class': 'tag_item'}).text
            re_lst = re.findall('\d+', release_time_str)
            release_time_raw = re_lst[0] + '-' + re_lst[1] + '-' + re_lst[2]
            release_time_ts = int(datetime.datetime.strptime(release_time_raw, '%Y-%m-%d').timestamp() * 1e3)
        except:
            release_time_ts = None
        try:
            duration_str = soup.find('meta', {'itemprop': 'duration'})['content']
            duration_str = duration_str.replace('PT', ':').replace('H', ':').replace('S', ':')
            duration = trans_duration(duration_str)
        except:
            duration = None
        return {"title": title,
                "video_intro": video_intro,
                "release_time": release_time_ts,
                "duration": duration}

    def process_video_page(self, resp_str):
        video_dict = {}
        resp_lst = resp_str.split('fangyuchenggoalkeeper')
        channel = resp_lst[0]
        play_count = int(resp_lst[1])
        url = resp_lst[2]
        page = resp_lst[3]
        soup = BeautifulSoup(page, 'html.parser')
        try:
            soup_find = soup.find("script", {"r-notemplate": "true"})
            midstep = soup_find.text
            video_info_var_Lst = re.findall('var\s+VIDEO_INFO\s+=\s*{.+}', midstep)
            video_info_var = video_info_var_Lst[0]
            video_info_json = re.sub('var\s+VIDEO_INFO\s+=\s*', '', video_info_var)
            video_info_dict = json.loads(video_info_json)
            if video_info_dict != {}:
                try:
                    duration_str = video_info_dict['duration']
                    duration = int(duration_str)
                except:
                    duration = None
                try:
                    title = video_info_dict['title']
                except:
                    title = None
                try:
                    release_time_str = video_info_dict['video_checkup_time']
                    release_time_ts = int(datetime.datetime.strptime(release_time_str,
                                                                     '%Y-%m-%d %H:%M:%S').timestamp() * 1e3)
                except:
                    release_time_ts = None
            else:
                video_dict = self.process_video_page_helper(soup)
        except:
            video_dict = self.process_video_page_helper(soup)
        try:
            releaser = soup.find('span', {'class': 'user_name'}).text
        except:
            releaser = None
            releaserUrl = None
        else:
            try:
                releaserUrl = soup.find('a', {'class': 'user_info'})['href']
            except:
                releaserUrl = None
        try:
            video_intro = soup.find('meta', {'itemprop': 'description'})['content']
        except:
            video_intro = None
        fetch_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1e3)
        if video_dict == {}:
            video_dict['title'] = title
            video_dict['video_intro'] = video_intro
            video_dict['duration'] = duration
            video_dict['release_time'] = release_time_ts
        video_dict['channel'] = channel
        video_dict['platform'] = self.platform
        video_dict['url'] = url
        video_dict['releaser'] = releaser
        video_dict['play_count'] = play_count
        video_dict['fetch_time'] = fetch_time
        video_dict['releaserUrl'] = releaserUrl
        video_dict['data_from'] = 'list_page'
        return video_dict

    def download_video_page_async_single_process(self):

        """get video page asynchronously in single process"""

        key = 'v_qq_url_dict'
        while connect_with_redis.length_of_set(key) > 0:
            task_lst = connect_with_redis.retrieve_url_dict_from_redis_set(platform=self.platform)
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.get_video_page(loop, task_lst=task_lst))

    def download_video_page_async_multi_process(self,
                                                process_num=10):
        pool = Pool(processes=process_num)
        for line in range(process_num):
            pool.apply_async(self.download_video_page_async_single_process)
        pool.close()
        pool.join()
        # self.loggerinfo.info('finish downloading video page on %s' % str(datetime.datetime.now()))

    def parse_video_page_single_process(self,
                                        output_to_file=False,
                                        filepath=None,
                                        push_to_redis=False,
                                        output_to_es_raw=True,
                                        es_index="crawler-data-raw",
                                        doc_type="doc",
                                        output_to_es_register=False, ):
        """
        parse download video page html in single process
        """
        result_lst = []
        count = 0
        pid = os.getpid()
        while connect_with_redis.length_of_lst(key='v_qq_video_page_html') > 0:
            resp_str = connect_with_redis.retrieve_video_page_html_from_redis(platform=self.platform)
            video_dic = self.process_video_page(resp_str=resp_str)
            if video_dic is not None:
                result_lst.append(video_dic)
                count += 1
                print("platform: %s, action: parse video page, process_id: %s, count number: %s"
                      % (self.platform, pid, count))
                if len(result_lst) >= 1000:
                    output_result(result_Lst=result_lst,
                                  platform=self.platform,
                                  output_to_file=output_to_file,
                                  push_to_redis=push_to_redis,
                                  filepath=filepath,
                                  output_to_es_raw=output_to_es_raw,
                                  es_index=es_index,
                                  doc_type=doc_type)
                    result_lst.clear()
        if result_lst != []:
            output_result(result_Lst=result_lst,
                          platform=self.platform,
                          output_to_file=output_to_file,
                          push_to_redis=push_to_redis,
                          filepath=filepath,
                          output_to_es_raw=output_to_es_raw,
                          es_index=es_index,
                          doc_type=doc_type)

    def parse_video_page_multi_process(self,
                                       para_dict,
                                       process_num=30):
        pool = Pool(processes=process_num)
        for line in range(process_num):
            pool.apply_async(self.parse_video_page_single_process, kwds=para_dict)
        pool.close()
        pool.join()
        # self.loggerinfo.info('finish parsing video page on %s' % str(datetime.datetime.now()))

    # renew video's play_count
    # this is for asynchronous video page crawler in the future due to its input and output
    async def get_video_page_html(self, session, url):
        """
        input video page's url to get url and video page html
        """
        get_page = await session.get(url)
        page = await get_page.text("utf-8", errors="ignore")
        return (url + 'fangyuchenggoalkeeper' + page)

    async def renew_video_play_count(self, loop, task_lst):
        async with aiohttp.ClientSession() as session:
            task_video_page = [loop.create_task(self.get_video_page_html(session,
                                                                         url)) for url in task_lst]
            result, unfinished = await asyncio.wait(task_video_page)
            html_lst = [v.result() for v in result]
            connect_with_redis.push_video_page_html_to_redis_renew(platform=self.platform,
                                                                   result_lst=html_lst)
            print("the length of url list is %s" % len(html_lst))

    def run_renew_video_play_count(self):
        """
        renew video play_count
        """
        task_lst = connect_with_redis.retrieve_video_url_from_redis_set(platform=self.platform)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.renew_video_play_count(loop, task_lst=task_lst))

    def run_renew_play_count_single_process(self):
        lst_key = connect_with_redis.platform_redis_set_reg[self.platform]
        while connect_with_redis.length_of_set(lst_key) > 0:
            self.run_renew_video_play_count()

    def run_renew_play_count_multi_process(self,
                                           process_num=10):
        pool = Pool(processes=process_num)
        for line in range(process_num):
            pool.apply_async(self.run_renew_play_count_single_process)
        pool.close()
        pool.join()

    def parse_video_html_to_renew_play_count(self, resp_str):
        video_dict = {}
        resp_lst = resp_str.split('fangyuchenggoalkeeper')
        url = resp_lst[0]
        page = resp_lst[1]
        soup = BeautifulSoup(page, 'html.parser')
        try:
            soup_find = soup.find("script", {"r-notemplate": "true"})
            midstep = soup_find.text
            video_info_var_Lst = re.findall('var\s+VIDEO_INFO\s+=\s*{.+}', midstep)
            video_info_var = video_info_var_Lst[0]
            video_info_json = re.sub('var\s+VIDEO_INFO\s+=\s*', '', video_info_var)
            video_info_dict = json.loads(video_info_json)
            duration_str = video_info_dict['duration']
            duration = int(duration_str)
            title = video_info_dict['title']
            play_count = video_info_dict['view_all_count']
            data_source = 'video_info'
            release_time_str = video_info_dict['video_checkup_time']
            release_time_ts = int(datetime.datetime.strptime(release_time_str,
                                                             '%Y-%m-%d %H:%M:%S').timestamp() * 1e3)
        except:
            video_dict = self.process_video_page_helper(soup)
            try:
                play_count_str = soup.find("meta", {"itemprop": "interactionCount"})["content"]
                play_count = int(play_count_str)
            except:
                play_count = 0
            data_source = 'interactioncount'
        try:
            releaser = soup.find('span', {'class': 'user_name'}).text
        except:
            releaser = None
            releaserUrl = None
        else:
            try:
                releaserUrl = soup.find('a', {'class': 'user_info'})['href']
            except:
                releaserUrl = None
        try:
            video_intro = soup.find('meta', {'itemprop': 'description'})['content']
        except:
            video_intro = None
        fetch_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1e3)
        if video_dict == {}:
            video_dict['title'] = title
            video_dict['video_intro'] = video_intro
            video_dict['duration'] = duration
            video_dict['release_time'] = release_time_ts
        video_dict['platform'] = self.platform
        video_dict['url'] = url
        video_dict['releaser'] = releaser
        video_dict['play_count'] = play_count
        video_dict['fetch_time'] = fetch_time
        video_dict['releaserUrl'] = releaserUrl
        video_dict['data_source'] = data_source
        video_dict['data_from'] = 'video_page'
        return video_dict

    def func_get_comment_count(self, url):
        headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "zh,zh-CN;q=0.9",
                "cache-control": "max-age=0",
                # "cookie": 'pgv_pvi=203414528; RK=SCQYJhGMVf; ptcz=5f0818b08a7345580a07bce669e0f0468b64107f4ecfb2c9bebf109cb23cf4fb; pgv_pvid=2754880744; ts_uid=176985184; tvfe_boss_uuid=54e907210062ff55; video_guid=0df27917cdb73abd; video_platform=2; XWINDEXGREY=0; mobileUV=1_16ac3c085a7_484c1; tvfe_search_uid=acc18029-4786-42c4-8f6a-f308777454bc; Qs_lvt_311470=1562066061; Qs_pv_311470=992309958717814400; _ga=GA1.2.1184421010.1562066062; login_remember=qq; ptui_loginuin=593516104; o_cookie=593516104; pac_uid=1_593516104; pgv_info=ssid=s8002196895; ied_qq=o0593516104; bucket_id=9231005; ts_refer=cn.bing.com/; ptag=cn_bing_com|channel; ts_last=v.qq.com/x/cover/mzc00200o70fhrw/r3047j4iuak.html; ad_play_index=83',
                "referer": "https://v.qq.com/channel/tech",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.87 Safari/537.36",
        }
        try:
            get_page = retry_get_url(url, headers=headers,
                                     timeout=self.timeout)
            try:
                comment_count = re.findall("(\d+)热评", get_page.text)[0]
            except:
                comment_count = re.findall("(\d+)条热评", get_page.text)[0]
            return int(comment_count)
        except:
            return 0

    def parse_video_page_to_renew_play_count_single_process(self,
                                                            output_to_file=False,
                                                            filepath=None,
                                                            output_to_es_raw=True,
                                                            output_to_es_register=False,
                                                            es_index="crawler-data-raw",
                                                            doc_type="doc"):
        """
        parse download video page html in single process
        """
        print(output_to_file, filepath,
              output_to_es_raw, output_to_es_register,
              es_index, doc_type)
        result_lst = []
        count = 0
        pid = os.getpid()
        redis_key = connect_with_redis.platform_redis_lst_reg[self.platform]
        while connect_with_redis.length_of_lst(lst_key=redis_key) > 0:
            resp_str = connect_with_redis.retrieve_video_html_from_redis_renew(self.platform)
            video_dic = self.parse_video_html_to_renew_play_count(resp_str)
            if video_dic is not None:
                result_lst.append(video_dic)
                count += 1
                print("the count number of process %s is %s" % (pid, count))
                if len(result_lst) >= 1000:
                    output_result(result_Lst=result_lst,
                                  platform=self.platform,
                                  output_to_file=output_to_file,
                                  filepath=filepath,
                                  output_to_es_raw=output_to_es_raw,
                                  es_index=es_index,
                                  doc_type=doc_type)
                    result_lst.clear()
        if result_lst != []:
            output_result(result_Lst=result_lst,
                          platform=self.platform,
                          output_to_file=output_to_file,
                          filepath=filepath,
                          output_to_es_raw=output_to_es_raw,
                          es_index=es_index,
                          doc_type=doc_type)

    #    def parse_video_page_to_renew_play_count_multiprocess(self,
    #                                                          output_to_file=False,
    #                                                          filepath=None,
    #                                                          output_to_es_raw=True,
    #                                                          output_to_es_register=False,
    #                                                          es_index="crawler-data-raw",
    #                                                          doc_type="doc",
    #                                                          process_num=30):
    #        print("start to parse video page html in multiprocessing")
    #        PARA_DIC = {'output_to_file': output_to_file,
    #                    'filepath': filepath,=None,
    #                                                          output_to_es_raw=True,
    #                                                          output_to_es_register=False,
    #                                                          es_index="crawler-data-raw",
    #                                                          doc_type="doc"
    #        pool = Pool(processes=process_num)
    #        for line in range(process_num):
    #            pool.apply_async(self.parse_video_page_to_renew_play_count_single_process, kwds=para_dic)
    #        pool.close()
    #        pool.join()

    # releaser_page asynchronous crawler
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

if __name__ == '__main__':
    test = Crawler_v_qq()
    url = 'http://v.qq.com/vplus/e03b859272d662175d9ec4604703c74d'
    # releaserUrl = 'http://v.qq.com/vplus/cfa34d96d1b6609f1dccdea65b26b83d'
    # nnn = test.video_page(url)
    # kw = '任正非 BBC'
    # #sr = test.search_page(kw, search_pages_max=2)
    res = test.releaser_page(url, output_to_es_raw=True,
                        es_index='crawler-data-raw',
                          doc_type='doc',
                         releaser_page_num_max=400)
    for re in res:
        continue
    # releaserUrl=url,)
    #test.get_releaser_follower_num("http://v.qq.com/vplus/cfa34d96d1b6609f1dccdea65b26b83d")
    test.releaser_page_by_time(1558972800000, 1562603029917, releaserUrl, output_to_es_raw=True,
                                      es_index='crawler-data-raw',
                                      doc_type='doc', releaser_page_num_max=4000)
    #test.get_releaser_image(releaserUrl)