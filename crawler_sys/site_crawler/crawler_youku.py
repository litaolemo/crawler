# -*- coding: utf-8 -*-
"""
Created on Sat Feb 24 14:19:00 2018

@author: fangyucheng
"""

import os
import time
import aiohttp
import asyncio
import requests
from bs4 import BeautifulSoup
import datetime
import re
import json
import copy
import random
# from selenium import webdriver
from multiprocessing import Pool
from multiprocessing import Process
from crawler_sys.framework.video_fields_std import Std_fields_video
from crawler_sys.utils.output_results import retry_get_url
from crawler_sys.utils.output_results import output_result
from crawler_sys.utils import trans_strtime_to_timestamp
from crawler.crawler_sys.utils import connect_with_redis
from crawler.crawler_sys.utils.trans_str_play_count_to_int import trans_play_count
from crawler.crawler_sys.utils.util_logging import logged


class Crawler_youku():

    def __init__(self, timeout=None, platform='youku'):
        if timeout==None:
            self.timeout = 10
        else:
            self.timeout = timeout
        self.platform = platform
        std_fields = Std_fields_video()
        self.video_data = std_fields.video_data
        self.video_data['platform'] = self.platform
        # remove fields that crawled data don't have
        pop_key_Lst = ['channel', 'describe', 'repost_count', 'isOriginal',
                       'video_id']
        for popk in pop_key_Lst:
            self.video_data.pop(popk)

        # with channel == '全部', url field is hard coded in list_page_all method,
        # input url will be ignored
        self.legal_channels = ['全部']
        self.list_page_url_dict = {
            '全部': '_',
            }

    def video_page(self, url):
        get_page = requests.get(url)
        get_page.encoding = 'utf-8'
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        releaser = soup.find('span',{'id': "module_basic_sub"}).a.text
        releaser = releaser.replace('\n', '')
        releaser = releaser.replace(' ', '')
        releaserUrl = soup.find('span',{'id': "module_basic_sub"}).a['href']
        if releaserUrl[0:4] == 'http':
            pass
        else:
            releaserUrl = 'http:' + releaserUrl
        try:
            rt_str = soup.find('span', {'class':'video-status'}).text
            rt_lst = re.findall('\d+', rt_str)
            rt_to_timestamp = str(rt_lst[0]) + '-' + str(rt_lst[1]) + '-' + str(rt_lst[2])
            release_time = int(datetime.datetime.strptime(rt_to_timestamp, '%Y-%m-%d').timestamp()*1e3)
        except:
            release_time = 0
        D0 = {'releaser':releaser,'releaserUrl':releaserUrl,'release_time':release_time}
        return D0

    def get_video_infor_by_network(self,url_Lst):
        video_id_Lst = []
        for url in url_Lst:
            video_id = ' '.join(re.findall('id.*html',url)).split('_')[1].split('.')[0]
            video_id_Lst.append(video_id)


    def get_video_title_play_count_duration(self, url):
        video_id = ' '.join(re.findall('id.*html', url))
        mobile_url = 'https://m.youku.com/video/' + video_id
        browser = webdriver.Chrome()
        browser.get(mobile_url)
        title = browser.find_element_by_class_name('online').text
        dura = browser.find_element_by_class_name('v-label').text.split('\n')[1]
        dura_lst = ' '.join(dura).split[':']
        if len(dura_lst) == 2:
            duration = dura_lst[0]*60 + dura_lst[1]
        elif len(dura_lst) == 3:
            duration = dura_lst[0]*3600 + dura_lst[1]*60 + dura_lst[2]
        pc_midstep1 = browser.find_element_by_class_name('video-num')
        pc_midstep2 = pc_midstep1.text
        pc_midstep3 = pc_midstep2.replace(',','')
        pc_midstep4 = re.findall('[0-9]+',pc_midstep3)
        if '万' in pc_midstep2:
            play_count = pc_midstep4[0]*1e4+pc_midstep4[1]*1e3
        else:
            play_count = ' '.join(pc_midstep4)
        fetch_time = int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
        D0 = {'video_id': video_id,
              'title': title,
              'duration': duration,
              'play_count': play_count,
              'url': url,
              'fetch_time': fetch_time}
        browser.close()
        return D0

    def get_video_play_count_by_vid(self, vid):
        mobile_url = 'https://m.youku.com/video/id_' + vid + '==.html'
        browser = webdriver.Chrome()
        browser.get(mobile_url)
        time.sleep(10)
        title = browser.find_element_by_class_name('online').text
        pc_midstep1 = browser.find_element_by_class_name('video-num')
        pc_midstep2 = pc_midstep1.text
        pc_midstep3 = pc_midstep2.replace(',','')
        pc_midstep4 = re.findall('[0-9]+', pc_midstep3)
        if '万' in pc_midstep2:
            play_count = pc_midstep4[0]*1e4+pc_midstep4[1]*1e3
        else:
            play_count = ' '.join(pc_midstep4)
        browser.close()
        D0 = {'title': title,
              'play_count': play_count}
        return D0

    def get_video_title_releaser_release_time(self, url):
        """An example of selenium
        """
        video_id = ' '.join(re.findall('id.*html', url))
        browser = webdriver.Chrome()
        browser.get(url)
        title = browser.find_element_by_id('subtitle').text
        releaser = browser.find_element_by_id('module_basic_sub').text
        releaser = releaser.replace('+订阅','')
        releaser = releaser.replace(' ','')
        try:
            rt_midstep = browser.find_element_by_class_name('video-status').text
            rt_midstep = rt_midstep.replace('上传于','')
            rt_midstep = rt_midstep.replace(' ','')
            release_time = int(datetime.datetime.strptime(rt_midstep,'%Y-%m-%d').timestamp()*1e3)
        except:
            release_time = 0
        fetch_time = int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
        D0 = {'video_id': video_id,
              'title': title,
              'release_time': release_time,
              'url': url,
              'fetch_time': fetch_time}
        return D0

    def list_page_sync(self, list_page_url, channel=None,
                  output_to_file=False, filepath=None,
                  output_to_es_raw=False,
                  output_to_es_register=False,
                  push_to_redis=False,
                  page_num_max=30):
        if channel == '全部':
            self.list_page_all(output_to_file=output_to_file, filepath=filepath,
                               output_to_es_raw=output_to_es_raw,
                               output_to_es_register=output_to_es_register,
                               push_to_redis=push_to_redis,
                               page_num_max=page_num_max)
        else:
            # need to be tested
            get_page = requests.get(list_page_url)
            page = get_page.text
            soup = BeautifulSoup(page,'html.parser')
            video = soup.find_all('div',{'class':'v-link'})
            dict_url = []
            for ttt in video:
                midstepurl=ttt.find('a')['href']
                if ('https:' in midstepurl):
                    midstepurl=midstepurl[6:]
                url = 'http:'+midstepurl
                dict_url.append(url)
            for xxxx in dict_url:
                if ("id" not in xxxx):
                    num=dict_url.index(xxxx)
                    dict_url.pop(num)
            for xxxx in dict_url:
                if ("id" not in xxxx):
                    num=dict_url.index(xxxx)
                    dict_url.pop(num)
            list_page_Lst = []
            for videourl in dict_url:
                one_video_dic = self.video_page(videourl)
                print ('one page done')
                list_page_Lst.append(one_video_dic)
            return list_page_Lst


    def search_page(self, keyword, search_pages_max=10,
                    output_to_es_raw=False,
                    output_to_es_register=False,
                    es_index=None,
                    doc_type=None):
        def process_one_line(data_line):
            title = data_line.find("div", {"class" : "v-meta-title"}).a['title']
            url = data_line.find("div", {"class" : "v-meta-title"}).a['href']
            releaser = data_line.find("span", {"class" : "username"}).text
            releaser = releaser.strip()
            releaserUrl = data_line.find("span", {"class" : "username"}).a['href']
            releaserUrl = 'http:' + releaserUrl
            play_count = data_line.find("span", {"class" : "pub"}).text
            dura = data_line.find("span", {"class" : "v-time"}).text
            duration_str = dura
            dl = duration_str.split(':')
            dl_int = []
            for v in dl:
                v = int(v)
                dl_int.append(v)
            if len(dl_int) == 2:
                duration = dl_int[0]*60+dl_int[1]
            else:
                duration = dl_int[0]*3660 + dl_int[1]*60+dl_int[2]
            videourl = 'https:' + url
            dict_page = self.video_page(videourl)
            release_time = dict_page['release_time']
            D0 = copy.deepcopy(self.video_data)
            D0['title'] = title
            D0['url'] = videourl
            D0['duration'] = duration
            D0['releaser'] = releaser
            D0['release_time'] = release_time
            D0['releaserUrl'] = releaserUrl
            D0['play_count'] = play_count

            return D0

        search_data_Lst = []
        jsurl = ('http://www.soku.com/search_video_ajax/q_'
                 + keyword
                 + '_orderby_1_limitdate_0?site=14&page={}'.format(
                     str(i)) for i in range (2, 4))
        for urls in jsurl:
            print(urls)
            get_page = requests.get(urls)
            print('get jspage done')
            page = get_page.text
            soup = BeautifulSoup(page,'html.parser')
            youku = soup.find_all("div", { "class" : "v" })
            for data_line in youku:
                one_video_dic = process_one_line(data_line)
                search_data_Lst.append(one_video_dic)
        url=('http://www.soku.com/search_video/q_'
             + keyword
             + '_orderby_1_limitdate_0?spm=a2h0k.8191407.0.0&site=14&page={}'.format(
                 str(i)) for i in range(1, search_pages_max+1))
        for urls in url:
            print(urls)
            get_page = requests.get(urls)
            print('get page done')
            page = get_page.text
            soup = BeautifulSoup(page, 'html.parser')
            youku = soup.find_all("div", {"class" : "v"})
            for data_line in youku:
                one_video_dic = process_one_line(data_line)
                search_data_Lst.append(one_video_dic)
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


    def get_releaser_detail(self, url):
        releaser_data_Lst = []
        get_page = requests.get(url)
        page = get_page.text
        soup = BeautifulSoup(page,'html.parser')
        releaser=soup.find('a',{'class':'username'})['title']
        releaser_intro=soup.find('span',{'class':'desc'}).text
        total_play_count=soup.find('li',{'class':'vnum'})['title']
        fans=soup.find('li',{'class':'snum'})['title']
        midstep=soup.find_all('div',{'class':'title'})
        for brother in midstep:
            if brother.h3.text=='视频':
                num=brother.find('span').text
                self.TotalVideo_num=num[1:-1]
        D_releaser_page={"releaser":releaser,"releaser_intro":releaser_intro,"total_play_count":total_play_count,"follow":fans,"TotalVideo_num":self.TotalVideo_num}
        releaser_data_Lst.append(D_releaser_page)
        return D_releaser_page


    def get_releaser_info(self, releaserUrl):
        get_page = retry_get_url(releaserUrl)
        page = get_page.text
        soup = BeautifulSoup(page,'html.parser')
        releaser = soup.find('a',{'class':'username'})['title']
        releaser_id = soup.find('a',{'class':'user-avatar'})['href']
        #total_video_num = soup.find('div',{'class':'title'}).text
        D0 = {'releaser':releaser, 'releaser_id':releaser_id}
        return D0

    @logged
    def releaser_page(self, releaserUrl, error_file=None,
                      output_to_file=False, filepath=None,
                      output_to_es_raw=False,
                      output_es_index=None,
                      output_doc_type=None,
                      output_to_es_register=False,
                      push_to_redis=False,
                      releaser_page_num_max=300):
        result_Lst = []
        releaser_info = self.get_releaser_info(releaserUrl)
        releaser = releaser_info['releaser']
        releaser_id = releaser_info['releaser_id']
        count = 1
        while count <= releaser_page_num_max:
            releaser_url = 'https://i.youku.com' + releaser_id + '/videos?&order=1&page=' + str(count)
            count += 1
            get_page = retry_get_url(releaser_url)
            get_page.encoding = 'utf-8'
            page = get_page.text
            soup = BeautifulSoup(page, 'html.parser')
            video_lst = soup.find_all("div", { "class" : "v va" })
            for one_video in video_lst:
                try:
                    title = one_video.find("div",{"class":"v-meta-title"}).a['title']
                    url = one_video.find('div',{'class':'v-meta-title'}).a['href']
                    if url[0:4] != 'http':
                        url = 'https:' + url
                    try:
                        play_count_text = one_video.find("span",{"class":"v-num"}).text
                        if '万' in play_count_text:
                            play_count_str = play_count_text.replace('万', '')
                            play_count = int(float(play_count_str)*1e4)
                        else:
                            play_count = int(play_count_text.replace(',', ''))
                    except:
                        play_count = 0
                    dura_str = one_video.find("span",{"class":"v-time"}).text
                    dura_lst = dura_str.split(':')
                    if len(dura_lst) == 2:
                        duration = int(dura_lst[0])*60 + int(dura_lst[1])
                    elif len(dura_lst) == 3:
                        duration = int(dura_lst[0])*3600 + int(dura_lst[1])*60 + int(dura_lst[2])
                    else:
                        duration = 0
                    rt_str = one_video.find('span',{'class':'v-publishtime'}).text
                    release_time = trans_strtime_to_timestamp.trans_strtime_to_timestamp(rt_str, missing_year=True)
                    fetch_time = int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)

                    self.video_data['title'] = title
                    self.video_data['duration'] = duration
                    self.video_data['url'] = url
                    self.video_data['play_count'] = play_count
                    self.video_data['releaser'] = releaser
                    self.video_data['releaserUrl'] = releaserUrl
                    self.video_data['release_time'] = release_time
                    self.video_data['fetch_time'] = fetch_time

                    get_data = copy.deepcopy(self.video_data)
                    result_Lst.append(get_data)
                    print('get_one_line %s' % url)
                except:
                    print('failed to get data')
                    pass
                if len(result_Lst)%100 == 0:
                    if output_doc_type is None and output_es_index is None:
                        output_result(result_Lst,
                                      self.platform,
                                      output_to_file=output_to_file,
                                      filepath=filepath,
                                      output_to_es_raw=output_to_es_raw,
                                      output_to_es_register=output_to_es_register,
                                      push_to_redis=push_to_redis)
                        result_Lst.clear()

                    elif output_doc_type is not None and output_es_index is not None:
                        output_result(result_Lst,
                                      self.platform,
                                      output_to_file=output_to_file,
                                      filepath=filepath,
                                      output_to_es_raw=output_to_es_raw,
                                      output_to_es_register=output_to_es_register,
                                      push_to_redis=push_to_redis,
                                      es_index=output_es_index,
                                      doc_type=output_doc_type)
                        result_Lst.clear()

        if result_Lst!=[]:
            if output_doc_type is None and output_es_index is None:
                output_result(result_Lst,
                              self.platform,
                              output_to_file=output_to_file,
                              filepath=filepath,
                              output_to_es_raw=output_to_es_raw,
                              output_to_es_register=output_to_es_register,
                              push_to_redis=push_to_redis)
            elif output_doc_type is not None and output_es_index is not None:
                output_result(result_Lst,
                              self.platform,
                              output_to_file=output_to_file,
                              filepath=filepath,
                              output_to_es_raw=output_to_es_raw,
                              output_to_es_register=output_to_es_register,
                              push_to_redis=push_to_redis,
                              es_index=output_es_index,
                              doc_type=output_doc_type)


    def doc_list(self, resultfile,donefile,doneurl,errorfile,area):
        list_data_lst=[]
        result=open(resultfile,'a')
        done=open(donefile,'a')
        donepage=open(doneurl,'a')
        error=open(errorfile,'a')
        count=1
        video_count=30
        while video_count==30 and count<=30:
            url='http://list.youku.com/category/show/c_84_a_'+area+'_s_1_d_1_p_'+str(count)+'.html'
            get_page=requests.get(url)
            get_page.encoding='utf-8'
            page = get_page.text
            print(url)
            soup = BeautifulSoup(page,'html.parser')
            midstep = soup.find_all('div',{'class':'p-thumb'})
            video_count=len(midstep)
            done.write(area)
            done.write(str(count))
            done.write('\n')
            done.flush()
            count+=1
            for line in midstep:
                url_step=line.a['href']
                if url_step[:4]!='http':
                    url='http:'+url_step
                else:
                    url=url_step
                try:
                    get_page=requests.get(url)
                    get_page.encoding='utf-8'
                    page = get_page.text
                    soup = BeautifulSoup(page,'html.parser')
                    print(url)
                    try:
                        donepage.write(url)
                        donepage.write('\n')
                        donepage.flush()
                        album_name=soup.find('h2').a.text
                        album_rating=soup.find('span',{'class':'score'}).text
                        sumvideo=soup.find('p',{'class':'tvintro'}).text
                        sumvideo_int=int(re.findall('\d+',sumvideo)[0])
                        if sumvideo_int<=100:
                            midstep=soup.find('div',{'id':'listitem_page1'})
                            first_video=midstep.find('div',{'class':'item item-cover current'})
                            url='http:'+first_video.a['href']
                            title=first_video['title']
                            dura=first_video.find('span',{'class':'c-time'}).text
                            dura_lst=dura.split(':')
                            if len(dura_lst)==1:
                                duration=int(dura_lst[0])
                            elif len(dura_lst)==2:
                                duration=int(dura_lst[0])*60+int(dura_lst[1])
                            elif len(dura_lst)==3:
                                duration=int(dura_lst[0])*3600+int(dura_lst[1])*60+int(dura_lst[2])
                            playcount_str=first_video.find('div',{'class':'status'}).text
                            playcount_str=playcount_str.replace('次播放','')
                            playcount_str=playcount_str.replace(',','')
                            thousand_10='万'
                            if thousand_10 in playcount_str:
                                playcount_str=playcount_str.replace('万','')
                                play_count=int((float(playcount_str))*1e4)
                            else:
                                play_count=int(playcount_str)
                            fetch_time=int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
                            D0={'url':url,'tilte':title,'duartion':duration,
                                'play_count':play_count,'album_name':album_name,
                                'album_rating':album_rating,'fetch_time':fetch_time}
                            D1={'url':url,'video_infor':D0}
                            list_data_lst.append(D1)
                            json_D1=json.dumps(D1)
                            result.write(json_D1)
                            result.write('\n')
                            result.flush()
                            print('get one line')
                            other_video=midstep.find_all('div',{'class':'item item-cover'})
                            for line in other_video:
                                url='http:'+first_video.a['href']
                                title=first_video['title']
                                dura=first_video.find('span',{'class':'c-time'}).text
                                dura_lst=dura.split(':')
                                if len(dura_lst)==1:
                                    duration=int(dura_lst[0])
                                elif len(dura_lst)==2:
                                    duration=int(dura_lst[0])*60+int(dura_lst[1])
                                elif len(dura_lst)==3:
                                    duration=int(dura_lst[0])*3600+int(dura_lst[1])*60+int(dura_lst[2])
                                playcount_str=first_video.find('div',{'class':'status'}).text
                                playcount_str=playcount_str.replace('次播放','')
                                playcount_str=playcount_str.replace(',','')
                                thousand_10='万'
                                if thousand_10 in playcount_str:
                                    playcount_str=playcount_str.replace('万','')
                                    play_count=int((float(playcount_str))*1e4)
                                else:
                                    play_count=int(playcount_str)
                                fetch_time=int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
                                D0={'url':url,'tilte':title,'duartion':duration,
                                'play_count':play_count,'album_name':album_name,
                                'album_rating':album_rating,'fetch_time':fetch_time}
                                D1={'url':url,'video_infor':D0}
                                list_data_lst.append(D1)
                                json_D1=json.dumps(D1)
                                result.write(json_D1)
                                result.write('\n')
                                print('get one line')
                            result.flush()
                        if sumvideo_int>100:
                            if sumvideo_int%50==0:
                                page_num=int((sumvideo_int/50))
                            else:
                                page_num=int((sumvideo_int)/50)+1
                            videoid=re.findall('\d+',(re.findall('videoId:"[0-9]{1,10}"',page)[0]))[0]
                            showid=re.findall('\d+',(re.findall('showid:"[0-9]{1,10}"',page)[0]))[0]
                            url_lst=['http://v.youku.com/page/playlist?&l=debug&pm=3&vid='+videoid+
                                     '&fid=0&showid='+showid+'&sid=0&page={}&callback=tuijsonp23'
                                     .format(str(i)) for i in range(1,page_num+1)]
                            for line in url_lst:
                                get_page = requests.get(line)
                                get_page.encoding = 'utf-8'
                                page = get_page.text
                                page_dic = eval(page[34:-2])
                                videolab = page_dic['html']
                                videolab = videolab.replace('\\','')
                                soup = BeautifulSoup(videolab,'html.parser')
                                first_video=soup.find('div',{'class':'item item-cover current'})
                                url='http:'+first_video.a['href']
                                title=first_video['title']
                                dura=first_video.find('span',{'class':'c-time'}).text
                                dura_lst=dura.split(':')
                                if len(dura_lst)==1:
                                    duration=int(dura_lst[0])
                                elif len(dura_lst)==2:
                                    duration=int(dura_lst[0])*60+int(dura_lst[1])
                                elif len(dura_lst)==3:
                                    duration=int(dura_lst[0])*3600+int(dura_lst[1])*60+int(dura_lst[2])
                                playcount_str=first_video.find('div',{'class':'status'}).text
                                playcount_str=playcount_str.replace('次播放','')
                                playcount_str=playcount_str.replace(',','')
                                thousand_10='万'
                                if thousand_10 in playcount_str:
                                    playcount_str=playcount_str.replace('万','')
                                    play_count=int((float(playcount_str))*1e4)
                                else:
                                    play_count=int(playcount_str)
                                fetch_time=int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
                                D0={'url':url,'tilte':title,'duartion':duration,
                                    'play_count':play_count,'album_name':album_name,
                                    'album_rating':album_rating,'fetch_time':fetch_time}
                                D1={'url':url,'video_infor':D0}
                                list_data_lst.append(D1)
                                json_D1=json.dumps(D1)
                                result.write(json_D1)
                                result.write('\n')
                                result.flush()
                                print('get one line')
                                other_video=midstep.find_all('div',{'class':'item item-cover'})
                                for line in other_video:
                                    url='http:'+first_video.a['href']
                                    title=first_video['title']
                                    dura=first_video.find('span',{'class':'c-time'}).text
                                    dura_lst=dura.split(':')
                                    if len(dura_lst)==1:
                                        duration=int(dura_lst[0])
                                    elif len(dura_lst)==2:
                                        duration=int(dura_lst[0])*60+int(dura_lst[1])
                                    elif len(dura_lst)==3:
                                        duration=int(dura_lst[0])*3600+int(dura_lst[1])*60+int(dura_lst[2])
                                    playcount_str=first_video.find('div',{'class':'status'}).text
                                    playcount_str=playcount_str.replace('次播放','')
                                    playcount_str=playcount_str.replace(',','')
                                    thousand_10='万'
                                    if thousand_10 in playcount_str:
                                        playcount_str=playcount_str.replace('万','')
                                        play_count=int((float(playcount_str))*1e4)
                                    else:
                                        play_count=int(playcount_str)
                                    fetch_time=int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
                                    D0={'url':url,'tilte':title,'duartion':duration,'play_count':play_count,
                                        'album_name':album_name,'album_rating':album_rating,'fetch_time':fetch_time}
                                    D1={'url':url,'video_infor':D0}
                                    list_data_lst.append(D1)
                                    json_D1=json.dumps(D1)
                                    result.write(json_D1)
                                    result.write('\n')
                                    print('get one line')
                                result.flush()
                    except:
                        error.write(url)
                        error.write('\n')
                        error.flush()
                        print('there is an error')
                except:
                    error.write(url)
                    error.write('\n')
                    error.flush()
                    print('there is an error')
        done.close()
        result.close()
        return list_data_lst

    def add_infor(self,taskname,resultname):
        task=open(taskname,'a')
        result=open(resultname,'a')
        task_lst=[]
        result_lst=[]
        for line in task:
            line_dic=eval(line)
            video_infor=line_dic['video_infor']
            task_lst.append(video_infor)
        for line in task_lst:
            url=line['url']
            try:
                video_dic=self.video_page(url)
                line['releaser']=video_dic['releaser']
                line['release_time']=video_dic['release_time']
                line['releaserUrl']=video_dic['releaserUrl']
                line['tags']=video_dic['tags']
                json_line=json.dumps(line)
                result_lst.append(line)
                result.write(json_line)
                result.write('\n')
                result.flush()
            except:
                print('no video infor')
        result.close()
        return result_lst

    def get_doc(self,cluename,taskname,resultname,donename):
        task=open(taskname)
        result=open(resultname,'a')
        clue=open(cluename,'a')
        done=open(donename,'a')
        task_lst=[]
        result_lst=[]
        for line in task:
            line_dic = eval(line)
            task_lst.append(line_dic)
        for line in task_lst:
            urls=line['url']
            urls=urls.replace('\n','')
            album_name=line['album_name']
            album_rating=line['album_rating']
            get_page = requests.get(urls)
            get_page.encoding = 'utf-8'
            page = get_page.text
            try:
                showid = (" ".join(re.findall('showid: &#\d+;\d+',page))).split(';')[1]
                videoId = (" ".join(re.findall('videoId: &#\d+;\d+',page))).split(';')[1]
                videoCategoryId = (" ".join(re.findall('videoCategoryId: &#\d+;\d+',page))).split(';')[1]
                componentid = (" ".join(re.findall('"componentId":\d+',page))).split(':')[1]
                D0={'showid':showid ,'videoid':videoId,'videocategoryid':videoCategoryId,'componentid':componentid}
                json_D0 = json.dumps(D0)
                clue.write(json_D0)
                clue.write('\n')
                clue.flush()
                count=1
                thousand_10='万'
                url='http://v.youku.com/page/playlist?&l=debug&pm=3&vid='+videoId+'&fid=0&showid='+showid+'&sid=0&componentid='+componentid+'&videoCategoryId='+videoCategoryId+'&isSimple=false&page='+str(count)
                get_page = requests.get(url)
                get_page.encoding = 'utf-8'
                page = get_page.text
                page_dic = eval(page)
                video_infor = page_dic['html']
                soup=BeautifulSoup(video_infor,'html.parser')
                video_dic = soup.find_all('div',{'class':'item item-cover'})
                sumofvideo = len(video_dic)
                for line in video_dic:
                    title=line['title']
                    url = line.a['href']
                    if 'http:' not in url:
                        url='http:'+url
                    dura=line.find('span',{'class':'c-time'}).text
                    dura_lst=dura.split(':')
                    if len(dura_lst)==1:
                        duration=int(dura_lst[0])
                    elif len(dura_lst)==2:
                        duration=int(dura_lst[0])*60+int(dura_lst[1])
                    elif len(dura_lst)==3:
                        duration=int(dura_lst[0])*3600+int(dura_lst[1])*60+int(dura_lst[2])
                    play_count_str=line.find('div',{'class':'status'}).text
                    play_count_str=play_count_str.replace('次播放','')
                    play_count_str=play_count_str.replace(',','')
                    if thousand_10 in play_count_str:
                        play_count_str=play_count_str.replace('万','')
                        play_count=int((float(play_count_str))*1e4)
                    else:
                        play_count=int(play_count_str)
                    fetch_time=int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
                    D1={'album_name':album_name,'album_rating':album_rating,'title':title,
                        'duration':duration,'play_count':play_count,'url':url,'fetch_time':fetch_time}
                    if D1 not in result_lst:
                        result_lst.append(D1)
                        json_D1 = json.dumps(D1)
                        result.write(json_D1)
                        result.write('\n')
                        result.flush()
                        print(url)
                while sumofvideo == 100:
                    count+=1
                    url='http://v.youku.com/page/playlist?&l=debug&pm=3&vid='+videoId+'&fid=0&showid='+showid+'&sid=0&componentid='+componentid+'&videoCategoryId='+videoCategoryId+'&isSimple=false&page='+str(count)
                    get_page = requests.get(url)
                    get_page.encoding = 'utf-8'
                    page = get_page.text
                    page_dic = eval(page)
                    video_infor = page_dic['html']
                    if video_infor =="\n":
                        sumofvideo=0
                    else:
                        soup=BeautifulSoup(video_infor,'html.parser')
                        video_dic = soup.find_all('div',{'class':'item item-cover'})
                        sumofvideo = len(video_dic)
                        for line in video_dic:
                            title=line['title']
                            url = line.a['href']
                            if 'http:' not in url:
                                url='http:'+url
                            dura=line.find('span',{'class':'c-time'}).text
                            dura_lst=dura.split(':')
                            if len(dura_lst)==1:
                                duration=int(dura_lst[0])
                            elif len(dura_lst)==2:
                                duration=int(dura_lst[0])*60+int(dura_lst[1])
                            elif len(dura_lst)==3:
                                duration=int(dura_lst[0])*3600+int(dura_lst[1])*60+int(dura_lst[2])
                            play_count_str=line.find('div',{'class':'status'}).text
                            play_count_str=play_count_str.replace('次播放','')
                            play_count_str=play_count_str.replace(',','')
                            if thousand_10 in play_count_str:
                                play_count_str=play_count_str.replace('万','')
                                play_count=int((float(play_count_str))*1e4)
                            else:
                                play_count=int(play_count_str)
                            fetch_time=int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
                            D1={'album_name':album_name,'album_rating':album_rating,'title':title,
                                'duration':duration,'play_count':play_count,'url':url,'fetch_time':fetch_time}
                            if D1 not in result_lst:
                                result_lst.append(D1)
                                json_D1 = json.dumps(D1)
                                result.write(json_D1)
                                result.write('\n')
                                result.flush()
                                print(url)
                done.write(urls)
                done.write('\n')
                done.flush()
            except:
                pass
        done.close()
        task.close()
        result.close()
        clue.close()
        return result_lst


    def one_video(self,taskname):
        task=open(taskname)
        task_lst=[]
        result_lst=[]
        for line in task:
            line_dic = eval(line)
            task_lst.append(line_dic)
        for line in task_lst:
            urls=line['url']
            urls=urls.replace('\n','')
            get_page = requests.get(urls)
            get_page.encoding = 'utf-8'
            page = get_page.text
            try:
                showid = (" ".join(re.findall('showid: &#\d+;\d+',page))).split(';')[1]
                print('more video')
            except:
                result_lst.append(urls)
                print('one video')
        task.close()
        return result_lst



    def get_releaser(self,taskname,resultname,errorname):
        task=open(taskname)
        result=open(resultname,'a')
        error=open(errorname,'a')
        task_lst=[]
        result_lst=[]
        for line in task:
            line_dic = eval(line)
            task_lst.append(line_dic)
        for line in task_lst:
            urls=line['url']
            get_page = requests.get(urls)
            get_page.encoding = 'utf-8'
            page = get_page.text
            page2 = page.replace('\\','')
            try:
                releaser=re.findall("[\u4e00-\u9fa5]+","".join(re.findall('class="" title="[\u4e00-\u9fa5]+"',page2)))[0]
                print('get releaser')
            except:
                releaser=None
                error.write(urls)
                error.write('\n')
                error.flush()
                print('no releaser')
            line['releaser']=releaser
            result_lst.append(line)
            json_line=json.dumps(line)
            result.write(json_line)
            result.write('\n')
            result.flush()
        result.close()
        return result_lst


    def get_release_time(self,url):
        get_page=requests.get(url)
        page=get_page.text
        soup = BeautifulSoup(page,'html.parser')
        try:
            time_one=soup.find('span',{'class':'bold mr3'}).text
        except AttributeError:
            time_one=None
        try:
            time_two=time_one[4:]
        except  TypeError:
            time_two=None
        try:
            midsteptime=time_two.strip()
        except  AttributeError:
            midsteptime=None
        try:
            release_time = int(datetime.datetime.strptime(midsteptime, '%Y-%m-%d').timestamp()*1e3)
        except  (TypeError,ValueError):
            release_time = None
        D_video_page={"release_time":release_time,'url':url}
        print('get one line')
        return D_video_page


    def get_video_play_count_releaser_info(self,url):
        get_page = requests.get(url)
        get_page.encoding = 'utf-8'
        page = get_page.text
        soup = BeautifulSoup(page,'html.parser')
        try:
            video_info = soup.find('div',{'class':'item item-cover current'})
            play_count_str = video_info.find('div',{'class':'status'}).text
            play_count_str = play_count_str.replace('次播放','')
            if '万' in play_count_str:
                play_count_str = play_count_str.replace('万','')
                play_count = int(float(play_count_str)*1e4)
            else:
                play_count = int(play_count_str.replace(',',''))
        except:
            play_count = 0
        try:
            releaser = soup.find('span',{'id':"module_basic_sub"}).a.text
            releaser = releaser.replace('\n','')
            releaser = releaser.replace(' ','')
            releaserUrl = soup.find('span',{'id':"module_basic_sub"}).a['href']
            if releaserUrl[0:4] == 'http':
                pass
            else:
                releaserUrl = 'http:' + releaserUrl
        except:
            releaser = None
            releaserUrl = None
        D0 = {'play_count':play_count, 'releaser':releaser, 'releaserUrl':releaserUrl}
        return D0


    def get_release_time_from_str(self,rt_str):
        minute = '分钟'
        hour = '小时'
        yesterday = '昨天'
        the_day_before_yesterday = '前天'
        the_day_before_many_days = '天前'
        if minute in rt_str or hour in rt_str :
            year = datetime.date.today().year
            month = datetime.date.today().month
            day = datetime.date.today().day
            connect_sign = '-'
            date = str(year) + connect_sign + str(month) + connect_sign + str(day)
            release_time = int(datetime.datetime.strptime(date,'%Y-%m-%d').timestamp()*1e3)
        elif yesterday in rt_str:
            raw_data = datetime.date.today() - datetime.timedelta(days = 1)
            year = raw_data.year
            month = raw_data.month
            day = raw_data.day
            connect_sign = '-'
            date = str(year) + connect_sign + str(month) + connect_sign + str(day)
            release_time = int(datetime.datetime.strptime(date,'%Y-%m-%d').timestamp()*1e3)
        elif the_day_before_yesterday in rt_str:
            raw_data = datetime.date.today() - datetime.timedelta(days = 2)
            year = raw_data.year
            month = raw_data.month
            day = raw_data.day
            connect_sign = '-'
            date = str(year) + connect_sign + str(month) + connect_sign + str(day)
            release_time = int(datetime.datetime.strptime(date,'%Y-%m-%d').timestamp()*1e3)
        elif the_day_before_many_days in rt_str:
            day_shift = int(re.findall('\d+',rt_str)[0])
            raw_data = datetime.date.today() - datetime.timedelta(days = day_shift)
            year = raw_data.year
            month = raw_data.month
            day = raw_data.day
            connect_sign = '-'
            date = str(year) + connect_sign + str(month) + connect_sign + str(day)
            release_time = int(datetime.datetime.strptime(date,'%Y-%m-%d').timestamp()*1e3)
        else:
            release_time = 0
        return release_time



    def list_page_all(self,
                      output_to_file=False, filepath=None,
                      output_to_es_raw=False,
                      output_to_es_register=False,
                      push_to_redis=False,
                      page_num_max=25):
        """
        Get all videos in youku.com '片库' in the home page bottom and the '全部'
        category with '最新发布' filter.
        At present (2018-06-28) the url is http://list.youku.com/category/video/c_0_d_1_s_2.html?spm=a2h1n.8251846.selectID.5!2~5~5!2~1~3!2~A
        This url changes to the following format
        'http://list.youku.com/category/video/c_0_d_1_s_2_p_' + str(count) + '.html'
        where count is page number.
        """
        # if page_num_max argument is passed in a value greater than 25, will use 25,
        # at present (2018-06-28) there is only 25 pages at most.
        if page_num_max > 25:
            page_num_max = 25
        result_Lst = []
        page_num = 1
        while page_num <= page_num_max:
            lst_url = 'http://list.youku.com/category/video/c_0_d_1_s_2_p_' + str(page_num) + '.html'
            print('Getting data on page %d: %s, %s'
                  % (page_num, lst_url, datetime.datetime.now()))
            get_page = retry_get_url(lst_url)
            if get_page is None:
                print('Failed to get page url: %s, will continue to next page.'
                      % lst_url)
                continue
            get_page.encoding = 'utf-8'
            page = get_page.text
            soup = BeautifulSoup(page, 'html.parser')
            video_agg = soup.find_all('div', {'class': 'yk-col4 '})
            for line in video_agg:
                title = line.find('li', {'class': 'title'}).text
                dura_str = line.find('li', {'class': 'status'}).text
                dura_lst = dura_str.split(':')
                if len(dura_lst) == 3:
                    duration = int(int(dura_lst[0])*3600 + int(dura_lst[1])*60 + int(dura_lst[2]))
                elif len(dura_lst) == 2:
                    duration = int(int(dura_lst[0])*60 + int(dura_lst[1]))
                else:
                    duration = 0
                url = line.a['href']
                if url[0:4] != 'http':
                    url = 'https:' + url
                video_info_dic = self.get_video_play_count_releaser_info(url)
                fetch_time = int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)

                rt_str = line.find('li', {'class': ' '}).text
                release_time = self.get_release_time_from_str(rt_str)

                self.video_data['title'] = title
                self.video_data['duration'] = duration
                self.video_data['url'] = url
                self.video_data['play_count'] = video_info_dic['play_count']
                self.video_data['releaser'] = video_info_dic['releaser']
                self.video_data['releaserUrl'] = video_info_dic['releaserUrl']
                self.video_data['release_time'] = release_time
                self.video_data['fetch_time'] = fetch_time

                get_data = copy.deepcopy(self.video_data)
                result_Lst.append(get_data)

                if len(result_Lst) >= 100:
                    print('Got %d lines, %s' % (len(result_Lst),
                                                datetime.datetime.now()))
                    output_result(result_Lst, self.platform,
                                  output_to_file=output_to_file,
                                  filepath=filepath,
                                  output_to_es_raw=output_to_es_raw,
                                  output_to_es_register=output_to_es_register,
                                  push_to_redis=push_to_redis)
                    result_Lst.clear()

            page_num += 1

        if result_Lst != []:
            print('Got %d lines, %s' % (len(result_Lst),
                                        datetime.datetime.now()))
            output_result(result_Lst, self.platform,
                          output_to_file=output_to_file,
                          filepath=filepath,
                          output_to_es_raw=output_to_es_raw,
                          output_to_es_register=output_to_es_register,
                          push_to_redis=push_to_redis)
            result_Lst.clear()

    def get_title_subtitle(self, url):
        get_page = retry_get_url(url)
        if get_page is None:
            print('Failed to get url: %s' % url)
            return
        get_page.encoding = 'utf-8'
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        try:
            subtitle = soup.find('span', {'id': 'subtitle'}).text
        except:
            subtitle = None
        try:
            title = soup.h1.find('a', {'target':'_blank'}).text
            title_str = title + subtitle
        except:
            title_str = subtitle
        return title_str

    def select_tv_series_video(self, sp_video_album_name,
                               result_file=None, # modify to from es
                               output_to_file=False,
                               filepath=None,
                               output_to_es_raw=False,
                               output_to_es_register=False,
                               push_to_redis=False):
        select_result = []
        meta = Meta()
        result_lst = meta.dic_file_to_lst(result_file)
        for line in result_lst:
            url = line['url']
            title_str = self.get_title_subtitle(url)
            if sp_video_album_name in title_str:
                line.update({'title': title_str})
                select_result.append(line)
                print('it_belongs_to_what_we_want')
                if len(select_result) >= 100:
                    print('Got %d lines, %s' % (len(select_result), datetime.datetime.now()))
                    output_result(select_result, self.platform,
                                  output_to_file=output_to_file,
                                  filepath=filepath,
                                  output_to_es_raw=output_to_es_raw,
                                  output_to_es_register=output_to_es_register,
                                  push_to_redis=push_to_redis)
                    select_result.clear()
            else:
                print('not_belongs_to')
        if select_result != []:
            print('Got %d lines, %s' % (len(select_result),datetime.datetime.now()))
            output_result(select_result, self.platform,
                          output_to_file=output_to_file,
                          filepath=filepath,
                          output_to_es_raw=output_to_es_raw,
                          output_to_es_register=output_to_es_register,
                          push_to_redis=push_to_redis)
            select_result.clear()



#list page asynchronous crawler
    def start_list_page(self, task_list,
                        output_to_file=False,
                        filepath=None,
                        output_to_es_raw=False,
                        push_to_redis=False,
                        es_index='crawler-data-raw',
                        doc_type='doc',
                        output_to_es_register=False,):
        self.list_page_task(task_list)
        self.download_list_page_single_process()


    def list_page_task(self, task_list, page_num_max=25):
        TASK_LIST = []
        for line in task_list:
            for num in range(1, page_num_max+1):
                url = line.replace('fangyucheng', str(num))
                TASK_LIST.append(url)
        random.shuffle(TASK_LIST)
        connect_with_redis.push_list_url_to_redis(platform=self.platform,
                                                  result_lst=TASK_LIST)

    async def download_page(self, session, url):
        get_page = await session.get(url)
        page = await get_page.text("utf-8", errors="ignore")
        return page

    async def download_list_page(self, loop):
        task_list = connect_with_redis.retrieve_list_url_from_redis(platform=self.platform)
        async with aiohttp.ClientSession() as session:
            task = [loop.create_task(self.download_page(session, url)) for url in task_list]
            done, pending = await asyncio.wait(task)
            RESULT_LIST = [d.result() for d in done]
            connect_with_redis.push_list_page_html_to_redis(platform=self.platform,
                                                            result_lst=RESULT_LIST)

    def download_list_page_single_process(self):
        key = 'youku_list_url'
        while connect_with_redis.length_of_lst(key) > 0:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.download_list_page(loop))

    def parse_list_page_single_process(self):
        pid = os.getpid()
        URL_LIST = []
        key = 'youku_list_page_html'
        while connect_with_redis.length_of_lst(key) > 0:
            PAGE = connect_with_redis.retrieve_list_page_html_from_redis(platform=self.platform)
            soup = BeautifulSoup(PAGE, 'html.parser')
            video_url_agg = soup.find_all('div', {'class': 'yk-col4 '})
            for line in video_url_agg:
                url = line.a['href']
                video_id_str = ' '.join(re.findall('id_.*html', url))
                video_id = video_id_str.replace('id_', '')
                tudou_url = 'https://video.tudou.com/v/%s' % video_id
                URL_LIST.append(tudou_url)
                connect_with_redis.push_video_url_to_redis_set(platform='new_tudou',
                                                               url_lst=URL_LIST)
            print("platform: %s, action: parse list page, process_id: %s, get %s urls"
                  % (self.platform, pid, len(URL_LIST)))

    def parse_list_page_multi_process(self, processes_num=20):
        pool = Pool(processes=processes_num)
        for num in range(processes_num):
            pool.apply_async(self.parse_list_page_single_process)
        pool.close()
        pool.join()


"""
due to the video play_count in new_tudou is equal to youku, and it is earier to get
play_count in new_tudou, I rebuild the youku video url so that I can use new_tudou
crawler to get data
"""
#    async def download_video_page(self, loop):
#        task_list = connect_with_redis.retrieve_video_url_from_redis_set(platform=self.platform)
#        async with aiohttp.ClientSession() as session:
#            task = [loop.create_task(self.download_page(session, url)) for url in task_list]
#            done, pending = await asyncio.wait(task)
#            RESULT_LIST = [d.result() for d in done]
#            connect_with_redis.push_video_page_html_to_redis(platform=self.platform,
#                                                             result_lst=RESULT_LIST)
#    def download_video_page_async_single_process(self):
#        key = 'youku_video_url'
#        pid = os.getpid()
#        while connect_with_redis.length_of_set(key) > 0:
#            loop = asyncio.get_event_loop()
#            loop.run_until_complete(self.download_video_page(loop))
#            print("platform: %s, action: download video page, process_id: %s"
#                  % (self.platform, pid))
#
#    def download_video_page_async_multi_process(self, processes_num=10):
#        pool = Pool(processes=processes_num)
#        for num in range(processes_num):
#            pool.apply_async(self.download_video_page_async_single_process)
#        pool.close()
#        pool.join()
#
#    def parse_video_page(self, resp_str):
#        soup = BeautifulSoup(resp_str, 'html.parser')
#        title = soup.find('meta', {'name': 'title'})['content']
#        url = soup.find('meta', {'itemprop': 'url'})['content']
#        release_time_str = soup.find('meta', {'itemprop': 'datePublished'})['content']
#        release_time = int(datetime.datetime.strptime(release_time_str,
#                                                      "%Y-%m-%d %H:%M:%S").timestamp()*1e3)
#        releaser = soup.find('span',{'id': "module_basic_sub"}).a.text
#        releaser = releaser.replace('\n', '')
#        releaser = releaser.replace(' ', '')
#        duration_str =  ' '.join(re.findall("seconds: '.*'", resp_str))
#        if '.' in duration_str:
#            duration_str = ' '.join(re.findall("seconds: '\d+.", duration_str))
#            duration = int(' '.join(re.findall('\d+', duration_str))) + 1
#        else:
#            duration = int(' '.join(re.findall('\d+', duration_str)))
#        releaserUrl = soup.find('span',{'id': "module_basic_sub"}).a['href']
#        if releaserUrl[0:4] != 'http':
#            releaserUrl = 'http:' + releaserUrl
#        try:
#            video_info = soup.find('div',{'class': 'item item-cover current'})
#            play_count_str = video_info.find('div',{'class': 'status'}).text
#            play_count = trans_play_count(play_count_str)
#        except:
#            print("can't find play count at %s" % url)
#            play_count = 0
#        try:
#            releaser = soup.find('span',{'id':"module_basic_sub"}).a.text
#            releaserUrl = soup.find('span',{'id':"module_basic_sub"}).a['href']
#        except:
#            releaser = None
#            releaserUrl = None
#        else:
#            releaser = releaser.replace('\n','')
#            releaser = releaser.replace(' ','')
#            if releaserUrl[0:4] != 'http':
#                releaserUrl = 'http:' + releaserUrl
#        fetch_time = int(time.time()*1e3)
#        video_info = {"title": title,
#                      'url': url,
#                      'duration': duration,
#                      'releaser': releaser,
#                      'play_count': play_count,
#                      'releaserUrl': releaserUrl,
#                      'release_time': release_time,
#                      'fetch_time': fetch_time}
#        return video_info
#
#    def parse_video_page_single_process(self, output_to_file=False,
#                                        filepath=None,
#                                        push_to_redis=False,
#                                        output_to_es_raw=False,
#                                        es_index=None,
#                                        doc_type=None,
#                                        output_to_es_register=False):
#        pid = os.getpid()
#        RESULT_LIST = []
#        key = 'youku_video_page_html'
#        while connect_with_redis.length_of_lst(key) > 0:
#            VIDEO_PAGE_HTML = connect_with_redis.retrieve_video_page_html_from_redis(platform=self.platform)
#            try:
#                video_info = self.parse_video_page(resp_str=VIDEO_PAGE_HTML)
#            except:
#                video_info = None
#                print("failed to parse video info")
#            if video_info is not None:
#                RESULT_LIST.append(video_info)
#                print("platform: %s, action: parse video page, process_id: %s, has done %s"
#                      % (self.platform, pid, len(RESULT_LIST)))
#                if len(RESULT_LIST) >= 100:
#                    output_result(result_Lst=RESULT_LIST,
#                                  platform=self.platform,
#                                  output_to_file=output_to_file,
#                                  filepath=filepath,
#                                  push_to_redis=push_to_redis,
#                                  output_to_es_raw=output_to_es_raw,
#                                  es_index=es_index,
#                                  doc_type=doc_type,
#                                  output_to_es_register=output_to_es_register)
#                    RESULT_LIST.clear()
#        if len(RESULT_LIST) != []:
#            output_result(result_Lst=RESULT_LIST,
#                          platform=self.platform,
#                          output_to_file=output_to_file,
#                          filepath=filepath,
#                          push_to_redis=push_to_redis,
#                          output_to_es_raw=output_to_es_raw,
#                          es_index=es_index,
#                          doc_type=doc_type,
#                          output_to_es_register=output_to_es_register)
#            RESULT_LIST.clear()
#
#    def parse_video_page_multi_process(self, para_dic,
#                                       processes_num=30):
#        pool = Pool(processes=processes_num)
#        for num in range(processes_num):
#            pool.apply_async(self.parse_video_page_single_process, kwds=para_dic)
#        pool.close()
#        pool.join()

# test
if __name__=='__main__':
    test = Crawler_youku()
    vid = 'XMjc5MTQyNjg0OA'
    test_selenium = test.get_video_play_count_by_vid(vid)
    sr_yk = test.search_page(keyword='任正非 BBC', search_pages_max=2)
