# -*- coding: utf-8 -*-
"""
Created on Mon Jul  9 09:35:59 2018

@author: fangyucheng
"""

import copy
import datetime
import re
import json
from bs4 import BeautifulSoup
from crawler_sys.framework.video_fields_std import Std_fields_video
from crawler_sys.utils.output_results import retry_get_url
from crawler_sys.utils.output_results import output_result

class Crawler_mango():


    def __init__(self, timeout=None, platform='Mango'):
        if timeout == None:
            self.timeout = 10
        else:
            self.timeout = timeout
        self.platform = platform
        std_fields = Std_fields_video()
        self.video_data = std_fields.video_data
        self.video_data['platform'] = self.platform
        # remove fields that crawled data don't have
        pop_key_Lst = ['channel', 'describe', 'repost_count', 'isOriginal',]
        for popk in pop_key_Lst:
            self.video_data.pop(popk)
        #self.releaser_url_pattern = 'http://www.365yg.com/c/user/[RELEASER_ID]/'


    def trans_dura_str_to_dura_int(self, dura_str):
        dura_lst = dura_str.split(':')
        if len(dura_lst) == 2:
            duration = int(dura_lst[0])*60 + int(dura_lst[1])
        elif len(dura_lst) == 3:
            duration = int(dura_lst[0])*3600 + int(dura_lst[1])*60 + int(dura_lst[2])
        else:
            duration = 0
        return duration


    def get_releaser_info(self, releaser_id):
        url = 'https://mguser.api.max.mgtv.com/artist/getArtistInfo?uid=' + releaser_id
        get_page = retry_get_url(url)
        get_page.encoding = 'utf-8'
        page = get_page.text
        page_dic = json.loads(page)
        releaser = page_dic['data']['nickName']
        return releaser


    def get_info_from_video_page(self, url):
        video_lst = []
        id_lst = []
        get_page = retry_get_url(url)
        get_page.encoding = 'utf-8'
        page = get_page.text
        try:
            releaser_id = re.findall('puuid:.*', page)[0].split('"')[1]
        except:
            releaser_id = None
        if len(releaser_id) > 5:
            releaserUrl = 'https://www.mgtv.com/u/' + releaser_id + '/d.html'
            releaser = self.get_releaser_info(releaser_id)
        else:
            releaserUrl = None
            releaser = None
        vid = re.findall('\d+', re.findall('vid:.*', page)[0])[0]
        video_info_url = ('https://pcweb.api.mgtv.com/common/list?video_id='
                          + vid + '&cxid=&version=5.5.35')
        get_video_page = retry_get_url(video_info_url)
        get_video_page.encoding = 'utf-8'
        video_page = get_video_page.text
        video_page_dic = json.loads(video_page)
        try:
            normal_video_lst = video_page_dic['data']['list']
        except:
            normal_video_lst = None
        try:
            short_video_lst = video_page_dic['data']['short']
        except:
            short_video_lst = None

        if normal_video_lst is not None:
            for line in normal_video_lst:
                title = line['t1']
                url = 'https://www.mgtv.com' + line['url']
                play_count = line['playcnt']
                video_id = line['video_id']
                clip_id = line['clip_id']
                rt_or_dura_str = line['t2']
                try:
                    release_time = int(datetime.datetime.strptime(rt_or_dura_str,
                                                                  '%Y-%m-%d').timestamp() * 1e3)
                    duration = 0
                except:
                    release_time = 0
                    duration = self.trans_dura_str_to_dura_int(rt_or_dura_str)
                else:
                    release_time = 0
                    duration = 0
                fetch_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1e3)

                self.video_data['title'] = title
                self.video_data['url'] = url
                self.video_data['play_count'] = play_count
                self.video_data['releaser'] = releaser
                self.video_data['releaserUrl'] = releaserUrl
                if release_time != 0:
                    self.video_data['release_time'] = release_time
                if duration != 0:
                    self.video_data['duration'] = duration
                self.video_data['fetch_time'] = fetch_time
                self.video_data['video_id'] = video_id
                self.video_data['clip_id'] = clip_id

                id_lst.append(video_id)
                get_data = copy.deepcopy(self.video_data)
                video_lst.append(get_data)
                print('get_one_long_video')

        if short_video_lst is not None:
            for line in short_video_lst:
                title = line['t1']
                url = 'https://www.mgtv.com' + line['url']
                play_count = line['playcnt']
                video_id = line['video_id']
                clip_id = line['clip_id']
                rt_or_dura_str = line['t2']
                try:
                    release_time = int(datetime.datetime.strptime(rt_or_dura_str,
                                                                  '%Y-%m-%d').timestamp() * 1e3)
                    duration = 0
                except:
                    release_time = 0
                    duration = self.trans_dura_str_to_dura_int(rt_or_dura_str)
                else:
                    release_time = 0
                    duration = 0
                fetch_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1e3)

                self.video_data['title'] = title
                self.video_data['url'] = url
                self.video_data['play_count'] = play_count
                self.video_data['releaser'] = releaser
                self.video_data['releaserUrl'] = releaserUrl
                if release_time != 0:
                    self.video_data['release_time'] = release_time
                if duration != 0:
                    self.video_data['duration'] = duration
                self.video_data['fetch_time'] = fetch_time
                self.video_data['video_id'] = video_id
                self.video_data['clip_id'] = clip_id

                id_lst.append(video_id)
                get_data = copy.deepcopy(self.video_data)
                video_lst.append(get_data)
                print('get_one_short')

        video_info_tup = (id_lst, video_lst)
        return video_info_tup


    def video_page(self, url):
        video_lst = self.get_info_from_video_page(url)[1]
        for line in video_lst:
            if url == line['url']:
                video_dic = line
        return video_dic


    def search_page(self,
                    keyword,
                    search_pages_max=30,
                    output_to_es_raw=False,
                    output_to_es_register=False,
                    es_index=None,
                    doc_type=None):
        processing_lst = []
        result_lst = []
        id_lst = []
        video_info_lst = []
        count = 1
        while count <= search_pages_max:
            search_url = 'https://so.mgtv.com/so/k-' + keyword + '?page=' + str(count)
            count += 1
            get_page = retry_get_url(search_url)
            get_page.encoding = 'utf-8'
            page = get_page.text
            soup = BeautifulSoup(page, 'html.parser')
            video_lst = soup.find_all('div', {'class':'so-result-info search-shortfilm clearfix'})
            for line in video_lst:
                title = line.find('img')['alt']
                url = 'https:' + line.find('a', {'class':'report-click report-action'})['href']
                video_id = line.find('a', {'class':'report-click report-action'})['video-id']
                clip_id = line.find('a', {'class':'report-click report-action'})['clip-id']
                dura_str = line.find('span', {'class':'rb'}).text
                duration = self.trans_dura_str_to_dura_int(dura_str)
                release_time_str = (line.find('span', {'class':'sho'}).text.replace('发布时间：', '').
                                    replace('\n', '').replace(' ', ''))
                release_time = int(datetime.datetime.strptime(release_time_str,
                                                              '%Y-%m-%d').timestamp()*1e3)
                fetch_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1e3)

                self.video_data['title'] = title
                self.video_data['url'] = url
                self.video_data['duration'] = duration
                self.video_data['release_time'] = release_time
                self.video_data['fetch_time'] = fetch_time
                self.video_data['video_id'] = video_id
                self.video_data['clip_id'] = clip_id

                get_data = copy.deepcopy(self.video_data)
                processing_lst.append(get_data)
                print('get_one_inmature_video')

        print('end of search_page')
        if video_info_lst == []:
            url = processing_lst[0]['url']
            print(url)
            video_tup = self.get_info_from_video_page(url)
            id_lst_son = video_tup[0]
            video_lst_son = video_tup[1]
            for line in id_lst_son:
                id_lst.append(line)
            for line in video_lst_son:
                video_info_lst.append(line)

        for line in processing_lst:
            if line['video_id'] in id_lst:
                for one_video in video_info_lst:
                    if line ['video_id'] == one_video['video_id']:
                        line['play_count'] = one_video['play_count']
                        line['title'] = one_video['title']
                        line['releaser'] = one_video['releaser']
                        line['releaserUrl'] = one_video['releaserUrl']
                        result_lst.append(line)
                        print('get_one_video')
                        if len(result_lst) >= 100:
                            output_result(result_Lst=result_lst,
                                          platform=self.platform,
                                          output_to_es_raw=output_to_es_raw,
                                          output_to_es_register=output_to_es_register,
                                          es_index=es_index,
                                          doc_type=doc_type)
                            result_lst.clear()
            else:
                url = line['url']
                video_tup = self.get_info_from_video_page(url)
                id_lst_son = video_tup[0]
                video_lst_son = video_tup[1]
                for video_id in id_lst_son:
                    id_lst.append(video_id)
                for video_info in video_lst_son:
                    video_info_lst.append(video_info)
                for one_video in video_info_lst:
                    if line ['video_id'] == one_video['video_id']:
                        line['play_count'] = one_video['play_count']
                        line['title'] = one_video['title']
                        line['releaser'] = one_video['releaser']
                        line['releaserUrl'] = one_video['releaserUrl']
                        result_lst.append(line)
                        print('get_one_video')
                        if len(result_lst) >= 100:
                            output_result(result_Lst=result_lst,
                                          platform=self.platform,
                                          output_to_es_raw=output_to_es_raw,
                                          output_to_es_register=output_to_es_register,
                                          es_index=es_index,
                                          doc_type=doc_type)
                            result_lst.clear()
        if len(result_lst) != []:
            output_result(result_Lst=result_lst,
                          platform=self.platform,
                          output_to_es_raw=output_to_es_raw,
                          output_to_es_register=output_to_es_register,
                          es_index=es_index,
                          doc_type=doc_type)
        print('success')
        return result_lst


# test
if __name__=='__main__':
    test = Crawler_mango()
    sr_mg = test.search_page(keyword='任正非 BBC', search_pages_max=2)
