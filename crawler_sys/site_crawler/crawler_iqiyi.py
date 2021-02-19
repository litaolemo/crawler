# !/usr/bin/python
# -*- coding: utf-8*-
"""
Created on Thu Mar  1 09:16:46 2018

@author: fangyucheng
"""

import os
import time
import asyncio
import aiohttp
import random
import datetime
import json
import re
import copy
import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool
from crawler.crawler_sys.framework.video_fields_std import Std_fields_video
from crawler.crawler_sys.utils.output_results import retry_get_url
from crawler.crawler_sys.utils.output_results import output_result
from crawler.crawler_sys.utils.trans_duration_str_to_second import trans_duration
from crawler.crawler_sys.utils import connect_with_redis
from crawler.crawler_sys.utils.util_logging import logged


class Crawler_iqiyi():

    def __init__(self, platform='iqiyi'):
        self.platform = platform
        std_fields = Std_fields_video()
        self.video_data = std_fields.video_data
        self.video_data['platform'] = self.platform
        # remove fields that crawled data don't have
        pop_key_Lst = ['describe', 'isOriginal', 'repost_count', 'video_id',
                       'channel', 'play_count']
        for popk in pop_key_Lst:
            self.video_data.pop(popk)

        self.list_page_Lst = [
            {'channel': '公益',
             'list_page_url': 'http://gongyi.iqiyi.com/',
             'page_type': 'gongyi'},
            {'channel': '电影',
             'list_page_url': 'http://list.iqiyi.com/www/1/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '片花',
             'list_page_url': 'http://list.iqiyi.com/www/10/1007----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '教育',
             'list_page_url': 'http://list.iqiyi.com/www/12/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '时尚',
             'list_page_url': 'http://list.iqiyi.com/www/13/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '儿童',
             'list_page_url': 'http://list.iqiyi.com/www/15/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '网络电影',
             'list_page_url': 'http://list.iqiyi.com/www/16/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '体育',
             'list_page_url': 'http://list.iqiyi.com/www/17/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '电视剧',
             'list_page_url': 'http://list.iqiyi.com/www/2/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '生活',
             'list_page_url': 'http://list.iqiyi.com/www/21/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '搞笑',
             'list_page_url': 'http://list.iqiyi.com/www/22/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '财经',
             'list_page_url': 'http://list.iqiyi.com/www/24/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '资讯',
             'list_page_url': 'http://list.iqiyi.com/www/25/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '汽车',
             'list_page_url': 'http://list.iqiyi.com/www/26/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '原创',
             'list_page_url': 'http://list.iqiyi.com/www/27/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '军事',
             'list_page_url': 'http://list.iqiyi.com/www/28/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '母婴',
             'list_page_url': 'http://list.iqiyi.com/www/29/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '纪录片',
             'list_page_url': 'http://list.iqiyi.com/www/3/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '科技',
             'list_page_url': 'http://list.iqiyi.com/www/30/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '脱口秀',
             'list_page_url': 'http://list.iqiyi.com/www/31/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '健康',
             'list_page_url': 'http://list.iqiyi.com/www/32/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '动漫',
             'list_page_url': 'http://list.iqiyi.com/www/4/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '音乐',
            'list_page_url': 'http://list.iqiyi.com/www/5/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '综艺',
             'list_page_url': 'http://list.iqiyi.com/www/6/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '娱乐',
             'list_page_url': 'http://list.iqiyi.com/www/7/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '旅游',
             'list_page_url': 'http://list.iqiyi.com/www/9/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
             {'channel': '广告',
             'list_page_url': 'http://list.iqiyi.com/www/20/----------------iqiyi--.html',
             'page_type': 'ordinary_list_page'},
            {'channel': '风云榜',
             'list_page_url': 'http://top.iqiyi.com',
             'page_type': 'top'},
            {'channel': '直播中心',
             'list_page_url': 'http://www.iqiyi.com/live/all',
             'page_type': 'live'},
            {'channel': '拍客',
             'list_page_url': 'http://www.iqiyi.com/paike/list/mrtj.html',
             'page_type': 'paike'},
            {'channel': '奇秀直播',
             'list_page_url': 'http://x.pps.tv/',
             'page_type': 'qixiu'}]
        self.list_page_url_dict = {}
        self.legal_list_page_urls = []
        self.legal_channels = []
        for lst_p in self.list_page_Lst:
            page_type = lst_p['page_type']
            channel = lst_p['channel']
            list_page_url = lst_p['list_page_url']
            if page_type == 'ordinary_list_page':
                self.list_page_url_dict[channel] = list_page_url
                self.legal_list_page_urls.append(list_page_url)
                self.legal_channels.append(channel)
            else:
                pass

    def rebuild_video_url(self, url):
        """
        get video page in mobile page instead of PC page
        """
        if "m.iqiyi.com" in url:
            return url
        elif "www.iqiyi.com" in url:
            url = url.replace("www.iqiyi.com", "m.iqiyi.com")
            return url

    def video_page(self, url, channel=None):
        """
        Due to iqiyi import hot index instead of play count,
        the crawler is updated on 2018-11-23
        """
        url = self.rebuild_video_url(url)
        start = time.time()
        get_page = retry_get_url(url)
        end = time.time() - start
        print("first request costs %s seconds" % end)
        if get_page is None:
            print('Failed to get html page for url: %s' % url)
            return None
        get_page.encoding = 'utf-8'
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        page_info = soup.find("div", {"is": "i71-play"})[":page-info"]
        page_dic = json.loads(page_info)
        title = page_dic["tvName"]
        url = page_dic["pageUrl"]
        dura_str = page_dic["duration"]
        duration = trans_duration(dura_str)
        try:
            releaser = page_dic["user"]["name"]
            releaserUrl = page_dic["user"]["profileUrl"]
        except:
            releaser = None
            releaserUrl = None
        video_info = soup.find("div", {"is": "i71-play"})[":video-info"]
        video_dic = json.loads(video_info)
        release_time = video_dic["firstPublishTime"]
        tvId = video_dic["tvId"]
        start1 = time.time()
        hot_idx_url = "https://pub.m.iqiyi.com/jp/h5/count/hotDisplay/?qipuId=%s" % tvId
        get_hot_idx = retry_get_url(hot_idx_url)
        end2 = time.time() - start1
        print("second request costs %s seconds" % end2)
        hot_idx_str = get_hot_idx.text
        hot_idx = int(re.findall("\d+", ' '.join(re.findall('"count":\d+', hot_idx_str)))[0])
        fetch_time = int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
        video_page_dict = copy.deepcopy(self.video_data)
        video_page_dict["title"] = title
        video_page_dict["url"] = url
        video_page_dict["duration"] = duration
        video_page_dict["releaser"] = releaser
        video_page_dict["releaserUrl"] = releaserUrl
        video_page_dict["release_time"] = release_time
        video_page_dict["hot_idx"] = hot_idx
        video_page_dict["fetch_time"] = fetch_time
        video_page_dict["tvId"] = tvId
        if channel is not None:
            video_page_dict["channel"] = channel
        return video_page_dict


    def redirect_by_js(self, url_raw):
        get_raw = retry_get_url(url_raw)
        if get_raw is not None:
            raw_page = get_raw.text
            soup = BeautifulSoup(raw_page, 'html.parser')
            soupf = soup.find_all(name='div', attrs={'class': 'cms-qipuId'})
            if soupf != []:
                data_qipuId = soupf[0].attrs['data-qipuid']
                url_redirect = 'http://www.iqiyi.com/v_%s.html' % data_qipuId
                return url_redirect
            else:
                return None
        else:
            print('Failed to get redirect by js for raw url: %s'
                  % url_raw)
            return None


    def search_page(self, keyword):
        search_page_Lst=[]
        def process_one_line(data_line):
            try:
                title=data_line.h3.a['title']
            except TypeError:
                title=None
            if title==None:
                title=None
            else:
                title=title.replace('\r','')
                title=title.replace('\n','')
                title=title.replace('"','“')
                title=title.replace(',','，')
            try:
                url=data_line.h3.a['href']
            except TypeError:
                url=None
            try:
                video_intro=data_line.find('span',{'class':'result_info_txt'}).text
            except AttributeError:
                video_intro=None
            try:
                 releaser=data_line('a',{'class':'result_info_link'}).text
            except  AttributeError:
                 releaser=None
            if releaser==None:
                releaserurl=None
            else:
                try:
                    releaserurl=data_line('a',{'class':'result_info_link'})
                except  TypeError:
                    releaserurl=None
            try:
                videofrom=data_line('em',{'class':'result_info_desc'}).text
            except  AttributeError:
                videofrom=None
            if video_intro==None:
                video_intro=None
            else:
                video_intro=video_intro.replace('\r','')
                video_intro=video_intro.replace('\n','')
                video_intro=video_intro.replace('"','“')
                video_intro=video_intro.replace(',','，')
            whether_continue=url[7:22]
            include_tag=['www.iqiyi.com/v','www.iqiyi.com/w']
            if whether_continue in include_tag:
                playcount=self.video_page(url)['playcount']
                release_time=self.video_page(url)['release_time']
                duration=self.video_page(url)['duration']
            else:
                playcount=None
                release_time=None
                duration=None
            D0={'title':title,'release_time':release_time,'url':url,'duration':duration,'video_intro':video_intro,'releaserurl':releaserurl,'videofrom':videofrom,'releaser':releaser,'playcount':playcount}
            return D0
        url=['http://so.iqiyi.com/so/q_'+keyword+'_ctg__t_0_page_{}'.format(str(i)) for i in range (1,21)]
        for urls in url:
            get_page=requests.get(urls)
            print (urls)
            get_page.encoding='utf-8'
            page=get_page.text
            soup = BeautifulSoup(page,'html.parser')
            iqiyi=soup.find_all('li',{'class':'list_item'})
            for data_line in iqiyi:
                one_video_dic=process_one_line(data_line)
                print ('get one line done')
                search_page_Lst.append(one_video_dic)
        return search_page_Lst

    def list_page_single(self, listurl, channel=None):
        """
        To be solved: video collection page, mainly include TV series
        and cartoon series, such as
        http://www.iqiyi.com/a_19rrh7z5vx.html#vfrm=2-4-0-1
        http://www.iqiyi.com/a_19rrk3ndgl.html#vfrm=2-4-0-1
        and zongyi show, such as
        http://www.iqiyi.com/a_19rrhbfb4d.html#vfrm=2-4-0-1
        """
        list_page_Lst = []
        get_page = retry_get_url(listurl)
        if get_page is None:
            print('Failed to get singe list page for url: %s'
                  % listurl)
            return None
        print (listurl)
        get_page.encoding = 'utf-8'
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        iqiyi = soup.find_all('div', {'class':'site-piclist_pic'})
        for data_line in iqiyi:
            try:
                url = data_line.find('a')['href']
            except TypeError:
                url = None
            if url is not None:
                video_page_dict = self.video_page(url, channel)
                if video_page_dict is not None:
                    list_page_Lst.append(video_page_dict)
                else:
                    print('Got None on video page url: %s' % url)
#                    # for test
#                    video_page_dict = {'url': url}
#                    list_page_Lst.append(video_page_dict)
            else:
                pass
        return list_page_Lst

    def get_paged_url_for_list_page(self, list_page_url_ori, page_num):
        """
        original list page url looks like
        http://list.iqiyi.com/www/$CHANNEL_NUM/----------------iqiyi--.html
        where $CHANNEL_NUM is an int represent one of the channels.
        With page number, url looks like
        http://list.iqiyi.com/www/$CHANNEL_NUM/-------------4-$PAGE_NUM-2-iqiyi--.html
        where $PAGE_NUM represent page number.
        """
        page_num_strs_dict = {
            '2': ['24', '1', '--'],
            '1': ['24', '1', '--'],
            '6': ['24', '1', '--'],
            '4': ['24', '1', '--'],
            '3': ['24', '', '-1-'],
            '8': ['24', '', '--'],
            '25': ['4', '2', '-1-'],
            '7': ['4', '2', '-1-'],
            '24': ['4', '', '--'],
            '16': ['24', '2', '-1-'],
            '10': ['24', '2', '-1-'],
            '5': ['24', '2', '--'],
            '28': ['4', '2', '-1-'],
            '12': ['24', '', '--'],
            '17': ['4', '2', '--'],
            '15': ['24', '1', '--'],
            '9': ['24', '', '--'],
            '13': ['4', '2', '-1-'],
            '21': ['4', '2', '-1-'],
            '26': ['24', '2', '-1-'],
            '22': ['24', '', '--'],
            '20': ['24', '2', '-1-'], # absent
            '27': ['24', '', '-1-'],
            '29': ['4', '', '--'],
            '30': ['24', '', '--'],
            '31': ['24', '', '--'],
            '32': ['24', '', '-1-'],
            }
        if list_page_url_ori not in self.legal_list_page_urls:
            print('Wrong list page url, must be one of %s'
                  % self.list_page_url_dict)
            return None
        else:
            channel_num_str_f = re.findall('www/[0-9]+/-{16}', list_page_url_ori)
            if channel_num_str_f != []:
                channel_num_str = channel_num_str_f[0].split('/')[1]
            else:
                # http://list.iqiyi.com/www/10/1007----------------iqiyi--.html
                # channel = '片花'
                channel_num_str_f = re.findall('www/[0-9]+/[0-9]+-{16}', list_page_url_ori)
                if channel_num_str_f != []:
                    channel_num_str = channel_num_str_f[0].split('/')[1]
                else:
                    print('Failed to extract channel number from url: %s'
                          % list_page_url_ori)
                    channel_num_str = None
            if channel_num_str is not None:
                page_str = '%s-%d-%s-iqiyi%s.html' % (page_num_strs_dict[channel_num_str][0],
                                  page_num,
                                  page_num_strs_dict[channel_num_str][1],
                                  page_num_strs_dict[channel_num_str][2])
                paged_url = re.sub('-{2}-iqiyi--.html', page_str, list_page_url_ori)
            else:
                paged_url = None

            return paged_url


    def list_page_sync(self, list_page_url, channel=None,
                       output_to_file=False, filepath=None,
                       output_to_es_raw=False,
                       output_to_es_register=False,
                       push_to_redis=False,
                       page_num_max=30):
        if list_page_url not in self.legal_list_page_urls:
            print('Wrong list page url, must be one of %s'
                  % self.list_page_url_dict)
            return None
        else:
            list_page_data = []
            first_page = self.list_page_single(list_page_url, channel)
            list_page_data.extend(first_page)
            page_num = 2
            while page_num <= page_num_max:
                paged_url = self.get_paged_url_for_list_page(list_page_url, page_num)
                if paged_url is not None:
                    paged_result = self.list_page_single(paged_url, channel)
                    list_page_data.extend(paged_result)
                    page_num += 1
                else:
                    print('Failed to form paged url for original list page url: %s'
                          % list_page_url)
                    return None
                if len(list_page_data) >= 100:
                    output_result(list_page_data,
                                  self.platform,
                                  output_to_file=output_to_file,
                                  filepath=filepath,
                                  output_to_es_raw=output_to_es_raw,
                                  output_to_es_register=output_to_es_register,
                                  push_to_redis=push_to_redis)
                    list_page_data.clear()
            #
            if list_page_data != []:
                output_result(list_page_data,
                              self.platform,
                              output_to_file=output_to_file,
                              filepath=filepath,
                              output_to_es_raw=output_to_es_raw,
                              output_to_es_register=output_to_es_register,
                              push_to_redis=push_to_redis)
                list_page_data.clear()
            return list_page_data


    def list_album_number(self,albumname,donename,oldurlname,listname,doneurllist,count):
        reflection={'figure':70, 'military':72,'history':74, 'exploration':73,'culture':77,
                  'society':71,'science':28119,'tour':310,'show':28137,'politics':28138}
        listname_num=reflection[listname]
        list_page_Lst=[]
        album=open(albumname, 'a')
        oldurl=open(oldurlname,'a')
        done=open(donename,'a')
        doneurl=open(doneurllist,'a')
        totalnum=30
        while totalnum>=30 and count<=30:
            listurl='http://list.iqiyi.com/www/3/'+str(listname_num)+'-------------11-'+str(count)+'-1-iqiyi-1-.html'
            get_page=requests.get(listurl)
            print (listurl)
            done.write(str(listname_num))
            done.write(str(count))
            done.write('\n')
            done.flush()
            get_page.encoding='utf-8'
            page=get_page.text
            soup = BeautifulSoup(page,'html.parser')
            iqiyi=soup.find_all('div',{'class':'site-piclist_pic'})
            totalnum=len(iqiyi)
            count+=1
            for line in iqiyi:
                album_name=soup.find('p',{'class':'site-piclist_info_title'}).a.text
                agg_url=soup.find('p',{'class':'site-piclist_info_title'}).a['href']
                aggid=line.a['data-qidanadd-albumid']
                sumvideo_str=line.find('span').text
                sumvideo_lst=re.findall('\d+',sumvideo_str)
                if len(sumvideo_lst)==1:
                    sumvideo=int(''.join(sumvideo_lst))
                    doneurl.write(agg_url)
                    doneurl.write('\n')
                    doneurl.flush()
                    if sumvideo/50==int(sumvideo/50):
                        pagenum=int(sumvideo/50)
                    else:
                        pagenum=int(sumvideo/50)+1
                    url_lst=['http://cache.video.iqiyi.com/jp/avlist/'+aggid+'/1/50/?albumId='+aggid+'&pageNum=50&pageNo={}'
                             .format(str(i)) for i in range (1,pagenum+1)]
                    for url in url_lst:
                        get_page=requests.get(url)
                        get_page.encoding='utf-8'
                        page=get_page.text
                        page_dic=eval(page[13:])
                        video_dic=page_dic['data']['vlist']
                        for line in video_dic:
                            url=line['vurl']
                            try:
                                one_video=self.video_page(url)
                                one_video['album_name']=album_name
                                one_video['subdomain']=listname
                                list_page_Lst.append(one_video)
                                print('get one line')
                                one_video_json=json.dumps(one_video)
                                album.write(one_video_json)
                                album.write('\n')
                            except:
                                D0={'url':url,'album_name':album_name,'subdomain':listname}
                                json_D0=json.dumps(D0)
                                oldurl.write(json_D0)
                                oldurl.write('\n')
                                oldurl.flush()
                        album.flush()
        album.close()
        oldurl.close()
        done.close()
        doneurl.close()
        return list_page_Lst

    def list_album_time(self,listurl,donename,albumname,oldurlname):
        list_page4=[]
        done=open(donename,'a')
        album=open(albumname,'a')
        oldurl=open(oldurlname,'a')
        monthlist=['01','02','03','04','05','06','07','08','09','10','11','12']
        for url in listurl:
            get_page=requests.get(url)
            get_page.encoding='utf-8'
            page=get_page.text
            soup = BeautifulSoup(page,'html.parser')
            year_agg=soup.find('div',{'class':'choose-years-list fl'}).find_all('a')
            ID=soup.find('div',{'class':'pianhuaPicList pianhuaPicList-noPlayNum'})['data-trailer-sidoraid']
            album_name=soup.find('h1').a['title']
            subdomain=soup.find('div',{'class':'right_col'}).text
            subdomain=subdomain.replace('\n','')
            subdomain=subdomain.replace('\r','')
            subdomain=subdomain.replace(' ','')
            done.write(url)
            done.write('\n')
            done.flush()
            yearlist=[]
            for line in year_agg:
                eachyear=line['data-year']
                yearlist.append(eachyear)
                for year in yearlist:
                    for month in monthlist:
                        url='http://cache.video.iqiyi.com/jp/sdvlst/3/'+ID+'/'+year+month+'/?&sourceId='+ID
                        try:
                            get_page=requests.get(url)
                            print(url)
                            get_page.encoding='utf-8'
                            page=get_page.text
                            page_dic=eval(page[13:])['data']
                            for line in page_dic:
                                url=line['vUrl']
                                try:
                                    one_video=self.video_page(url)
                                    one_video['album_name']=album_name
                                    one_video['subdomain']=subdomain
                                    print('get one line done')
                                    list_page4.append(one_video)
                                    video=json.dumps(one_video)
                                    album.write(video)
                                    album.write('\n')
                                except:
                                    D0={'album_name':album_name,'subdomain':subdomain,'url':url}
                                    json_D0=json.dumps(D0)
                                    oldurl.write(json_D0)
                                    oldurl.write('\n')
                                    oldurl.flush()
                            album.flush()
                        except:
                            pass
        album.close()
        oldurl.close()
        done.close()
        return list_page4


    def oldpage(self,url):
        wr=open('old','a')
        pr=open('pro','a')
        get_page=requests.get(url)
        print (url)
        get_page.encoding='utf-8'
        page=get_page.text
        soup = BeautifulSoup(page,'html.parser')
        try:
            try:
                midstep=soup.body.div['data-qipuid']
            except:
                midstep=soup.body.div['data-qipuId']
            newurl='http://www.iqiyi.com/v_'+midstep+'.html'
            one_video=self.video_page(newurl)
            one_video_json=json.dumps(one_video)
            print(newurl)
            wr.write(one_video_json)
            wr.write('\n')
            wr.flush()
        except:
            pr.write(url)
            pr.write('\n')
            pr.flush()
        wr.close()
        pr.close()


    def rebuild_releaserUrl(self, releaserUrl):
        get_releaser_id = ' '.join(re.findall('/u/\d+/', releaserUrl))
        releaser_id = ' '.join(re.findall('\d+', get_releaser_id))
        return str(releaser_id)


    @logged
    def releaser_page(self, releaserUrl,
                      output_to_file=False,
                      filepath=None,
                      output_to_es_raw=False,
                      output_to_es_register=False,
                      push_to_redis=False,
                      releaser_page_num_max=30,
                      es_index=None,
                      doc_type=None):
        videos_per_page = 42
        releaser_page_Lst = []
        releaser_id = self.rebuild_releaserUrl(releaserUrl)
        if releaser_id == '' or releaser_id is None:
            print('Failed to get releaser id: %s' % releaserUrl)
            return None
        real_releaserUrl = 'https://www.iqiyi.com/u/'+releaser_id+'/v'
        get_page = retry_get_url(real_releaserUrl)
        if get_page is None:
            print('Failed to get releaser page: %s' % releaserUrl)
            return None
        get_page.encoding = 'utf-8'
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        try:
            videonum_str = soup.find('span', {'class': 'icon-num'}).text
            videonum_f = re.findall('[0-9]+', videonum_str)
        except:
            print ('Failed to get total video number: %s' % releaserUrl)
            videonum_f = []
        if videonum_f != []:
            videonum = int(videonum_f[0])
            totalpage = videonum // videos_per_page + 1
        else:
            videonum = None
            totalpage = 1000 # assign an arbitary number

        def process_one_line(data_line):
            url = data_line.find('p', {'class':'site-piclist_info_title_twoline'}).a['href']
            if url[:6] != 'https:' or url[:5] != 'http:':
                url = 'https:' + url
            get_video_dict = self.video_page(url)
            if get_video_dict is None:
                return None
            return get_video_dict

        releaser_url_body_f = re.findall('https://www.iqiyi.com/u/[0-9]+/v', releaserUrl)
        if releaser_url_body_f != []:
            releaser_url_body = releaser_url_body_f[0]
        else:
            releaser_url_body_f = re.findall('http://www.iqiyi.com/u/[0-9]+/v', releaserUrl)
        if releaser_url_body_f != []:
            releaser_url_body = releaser_url_body_f[0]
        else:
            return None

        if releaser_page_num_max > totalpage:
            releaser_page_num_max = totalpage
        else:
            pass
        video_page_url = [releaser_url_body
                          + '?page={}&video_type=1'.format(str(i)) for i in range(1, releaser_page_num_max+1)]
        for urls in video_page_url:
            get_page = retry_get_url(urls)
            if get_page is None:
                continue
            print("get %s successfully" % urls)
            page = get_page.text
            soup = BeautifulSoup(page,'html.parser')
            iqiyi = soup.find_all('li', {'j-delegate': 'colitem'})
            for data_line in iqiyi:
                one_video_dic = process_one_line(data_line)
                releaser_page_Lst.append(one_video_dic)
                if len(releaser_page_Lst) >= 100:
                    output_result(releaser_page_Lst, self.platform,
                                  output_to_file=output_to_file,
                                  filepath=filepath,
                                  output_to_es_raw=output_to_es_raw,
                                  output_to_es_register=output_to_es_register,
                                  push_to_redis=push_to_redis,
                                  es_index=es_index,
                                  doc_type=doc_type)
                    releaser_page_Lst.clear()
        if releaser_page_Lst != []:
            output_result(releaser_page_Lst, self.platform,
                          output_to_file=output_to_file,
                          filepath=filepath,
                          output_to_es_raw=output_to_es_raw,
                          output_to_es_register=output_to_es_register,
                          push_to_redis=push_to_redis,
                          es_index=es_index,
                          doc_type=doc_type)



    def list_page_number2(self,albumname,donename,oldurlname,listname):
        reflection={'figure':70, 'military':72,'history':74, 'exploration':73,'culture':77,
                  'society':71,'science':28119,'tour':310,'show':28137,'politics':28138}
        listname_num=reflection[listname]
        list_page_Lst=[]
        album=open(albumname, 'a')
        oldurl=open(oldurlname,'a')
        done=open(donename,'a')
        totalnum=30
        count=1
        while totalnum>=30 and count<=30:
            listurl='http://list.iqiyi.com/www/3/'+str(listname_num)+'-------------11-'+str(count)+'-1-iqiyi-1-.html'
            get_page=requests.get(listurl)
            print (listurl)
            done.write(str(listname_num))
            done.write(str(count))
            done.write('\n')
            done.flush()
            get_page.encoding='utf-8'
            page=get_page.text
            soup = BeautifulSoup(page,'html.parser')
            iqiyi=soup.find_all('div',{'class':'site-piclist_pic'})
            totalnum=len(iqiyi)
            count+=1
            for line in iqiyi:
                album_name=soup.find('p',{'class':'site-piclist_info_title'}).a.text
                aggid=line.a['data-qidanadd-albumid']
                sumvideo_str=line.find('span').text
                sumvideo_lst=re.findall('\d+',sumvideo_str)
                if len(sumvideo_lst)==1:
                    sumvideo=int(''.join(sumvideo_lst))
                    if sumvideo/50==int(sumvideo/50):
                        pagenum=int(sumvideo/50)
                    else:
                        pagenum=int(sumvideo/50)+1
                    url_lst=['http://cache.video.iqiyi.com/jp/avlist/'+aggid+'/1/50/?albumId='+aggid+'&pageNum=50&pageNo={}'
                             .format(str(i)) for i in range (1,pagenum+1)]
                    for url in url_lst:
                        get_page=requests.get(url)
                        get_page.encoding='utf-8'
                        page=get_page.text
                        page_dic=eval(page[13:])
                        video_dic=page_dic['data']['vlist']
                        for line in video_dic:
                            url=line['vurl']
                            try:
                                one_video=self.video_page(url)
                                one_video['album_name']=album_name
                                one_video['subdomain']=listname
                                list_page_Lst.append(one_video)
                                print('get one line')
                                one_video_json=json.dumps(one_video)
                                album.write(one_video_json)
                                album.write('\n')
                            except:
                                oldurl.write(url)
                                oldurl.write('\n')
                                oldurl.flush()
                        album.flush()
        album.close()
        oldurl.close()
        done.close
        return list_page_Lst


    def review_album(self,albumname,donename,subdomain):
        reflection={'figure':70, 'military':72,'history':74, 'exploration':73,'culture':77,
                  'society':71,'science':28119,'tour':310,'show':28137,'politics':28138}
        listname=reflection[subdomain]
        list_page_Lst=[]
        album=open(albumname, 'a')
        done=open(donename,'a')
        totalnum=30
        count=1
        while totalnum>=30 and count<=30:
            listurl='http://list.iqiyi.com/www/3/'+str(listname)+'-------------11-'+str(count)+'-1-iqiyi-1-.html'
            get_page=requests.get(listurl)
            print (listurl)
            done.write(str(listname))
            done.write(str(count))
            done.write('\n')
            done.flush()
            get_page.encoding='utf-8'
            page=get_page.text
            soup = BeautifulSoup(page,'html.parser')
            iqiyi=soup.find_all('div',{'class':'site-piclist_pic'})
            totalnum=len(iqiyi)
            count+=1
            for line in iqiyi:
                album_name='miss'
                album_name=line.a['title']
                aggid=line.a['data-qidanadd-albumid']
                sumvideo_str=line.find('span').text
                sumvideo_lst=re.findall('\d+',sumvideo_str)
                if len(sumvideo_lst)==1:
                    sumvideo=int(''.join(sumvideo_lst))
                    if sumvideo/50==int(sumvideo/50):
                        pagenum=int(sumvideo/50)
                    else:
                        pagenum=int(sumvideo/50)+1
                    url_lst=['http://cache.video.iqiyi.com/jp/avlist/'+aggid+'/1/50/?albumId='+aggid+'&pageNum=50&pageNo={}'
                             .format(str(i)) for i in range (1,pagenum+1)]
                    for url in url_lst:
                        get_page=requests.get(url)
                        get_page.encoding='utf-8'
                        page=get_page.text
                        page_dic=eval(page[13:])
                        video_dic=page_dic['data']['vlist']
                        for line in video_dic:
                            url=line['vurl']
                            D0={'url':url,'album_name':album_name,'subdomain':subdomain}
                            list_page_Lst.append(D0)
                            print('get one line')
                            D0_json=json.dumps(D0)
                            album.write(D0_json)
                            album.write('\n')
                        album.flush()
        album.close()
        done.close
        return list_page_Lst


    def oldurl(self, url_lst):
        refresh = []
        for url in url_lst:
            get_page = requests.get(url)
            get_page.encoding = 'utf-8'
            page = get_page.text
            soup = BeautifulSoup(page,'html.parser')
            new_url = soup.body.div['data-qipuid']
            refresh.append(new_url)
            print(new_url)
        return refresh


#from here is list page crawler asychronous
    def start_list_page(self, task_list):
        self.list_page_task(task_list)
        self.download_list_page_single_process()

    def list_page_task(self, task_list,
                       page_num_max=30):
        LIST_PAGE_TASK_LIST = []
        for list_url in task_list:
            for num in range(1, page_num_max+1):
                LIST_URL = list_url.replace('fangyucheng', str(num))
                LIST_PAGE_TASK_LIST.append(LIST_URL)
        random.shuffle(LIST_PAGE_TASK_LIST)
        connect_with_redis.push_list_url_to_redis(platform=self.platform,
                                                  result_lst=LIST_PAGE_TASK_LIST)

    async def download_page(self, session, url):
        get_page = await session.get(url)
        page = await get_page.text("utf-8", errors="ignore")
        return page

    async def get_list_page(self, loop):
        task_list = connect_with_redis.retrieve_list_url_from_redis(platform=self.platform)
        async with aiohttp.ClientSession() as list_page_sess:
            task = [loop.create_task(self.download_page(list_page_sess, url)) for url in task_list]
            done, pending = await asyncio.wait(task)
            result_lst = [d.result() for d in done]
            connect_with_redis.push_list_page_html_to_redis(platform=self.platform,
                                                            result_lst=result_lst)

    def download_list_page_single_process(self):
        key = 'iqiyi_list_url'
        while connect_with_redis.length_of_lst(key) > 0:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.get_list_page(loop))

    def parse_list_page_single_process(self):
        pid = os.getpid()
        key = 'iqiyi_list_page_html'
        while connect_with_redis.length_of_lst(key) > 0:
            LIST_PAGE_HTML = connect_with_redis.retrieve_list_page_html_from_redis(platform=self.platform)
            URL_LIST = []
            soup = BeautifulSoup(LIST_PAGE_HTML, 'html.parser')
            SOUP_FIND = soup.find_all('div', {'class':'site-piclist_pic'})
            for line in SOUP_FIND:
                try:
                    url = line.find('a')['href']
                    URL_LIST.append(url)
                except:
                    pass
            print("platform: %s, action: parse list page, process_id: %s, get %s urls"
                  % (self.platform, pid, len(URL_LIST)))
            connect_with_redis.push_video_url_to_redis_set(platform=self.platform,
                                                           url_lst=URL_LIST)

    def parse_list_page_multi_process(self, process_num=20):
        pool = Pool(processes=process_num)
        for line in range(process_num):
            pool.apply_async(self.parse_list_page_single_process)
        pool.close()
        pool.join()

    async def get_video_page(self, loop):
        task_list = connect_with_redis.retrieve_video_url_from_redis_set(platform=self.platform)
        async with aiohttp.ClientSession() as video_page_sess:
            task = [loop.create_task(self.download_page(video_page_sess,
                                                        url)) for url in task_list]
            done, pending = await asyncio.wait(task)
            result_lst = [d.result() for d in done]
            connect_with_redis.push_video_page_html_to_redis(platform=self.platform,
                                                             result_lst=result_lst)

    def download_video_page_async_single_process(self):
        pid = os.getpid()
        key = 'iqiyi_video_url'
        while connect_with_redis.length_of_set(key) > 0:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.get_video_page(loop))
            print("platform: %s, action: download video page, process_id: %s"
                  % (self.platform, pid))

    def download_video_page_async_multi_process(self, process_num=10):
        pool = Pool(processes=process_num)
        for line in range(process_num):
            pool.apply_async(self.download_video_page_async_single_process)
        pool.close()
        pool.join()

    def parse_video_page_single_process(self,
                                        output_to_file=False,
                                        filepath=None,
                                        push_to_redis=False,
                                        output_to_es_raw=True,
                                        es_index="crawler-data-raw",
                                        doc_type="doc",
                                        output_to_es_register=False):
        key = 'iqiyi_video_page_html'
        result_list = []
        pid = os.getpid()
        while connect_with_redis.length_of_lst(key) > 0:
            video_page_html = connect_with_redis.retrieve_video_page_html_from_redis(platform=self.platform)
            soup = BeautifulSoup(video_page_html, 'html.parser')
            try:
                page_info = soup.find("div", {"is": "i71-play"})[":page-info"]
                page_info = page_info.replace("'", '"')
                page_dic = json.loads(page_info)
            except:
                page_dic = None
            if page_dic is not None:
                title = page_dic["tvName"]
                url = page_dic["pageUrl"]
                dura_str = page_dic["duration"]
                duration = trans_duration(dura_str)
                try:
                    releaser = page_dic["user"]["name"]
                    releaserUrl = page_dic["user"]["profileUrl"]
                except:
                    releaser = None
                    releaserUrl = None
            else:
                title = None
                url = None
                duration = None
                releaser = None
                releaserUrl = None
            try:
                video_info = soup.find("div", {"is": "i71-play"})[":video-info"]
                video_dic = json.loads(video_info)
            except:
                video_dic = None
            if video_dic is not None:
                if title is None:
                    title = video_dic['name']
                if url is None:
                    url = video_dic['url']
                if releaser is None:
                    try:
                        releaser = video_dic["user"]["name"]
                        releaserUrl = video_dic["user"]["profileUrl"]
                    except:
                        releaser = None
                        releaserUrl = None
                release_time = video_dic["firstPublishTime"]
                tvId = video_dic["tvId"]
                hot_idx_url = "https://pub.m.iqiyi.com/jp/h5/count/hotDisplay/?qipuId=%s" % tvId
                get_hot_idx = retry_get_url(hot_idx_url)
                hot_idx_str = get_hot_idx.text
                hot_idx = int(re.findall("\d+", ' '.join(re.findall('"count":\d+', hot_idx_str)))[0])
            fetch_time = int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
            if releaser is None:
                try:
                    releaser = soup.find('span', {'class': 'intro-iterm__txt'}).text
                except:
                    releaser = None
            video_page_dict = copy.deepcopy(self.video_data)
            video_page_dict["title"] = title
            video_page_dict["url"] = url
            video_page_dict["duration"] = duration
            video_page_dict["releaser"] = releaser
            video_page_dict["releaserUrl"] = releaserUrl
            video_page_dict["release_time"] = release_time
            video_page_dict["hot_idx"] = hot_idx
            video_page_dict["fetch_time"] = fetch_time
            video_page_dict["tvId"] = tvId
            result_list.append(video_page_dict)
            print("platform: %s, action: parse video page, process_id: %s, has done: %s"
                  % (self.platform, pid, len(result_list)))
            if len(result_list) >= 1000:
                output_result(result_Lst=result_list,
                              platform=self.platform,
                              output_to_file=output_to_file,
                              filepath=filepath,
                              push_to_redis=push_to_redis,
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
                          push_to_redis=push_to_redis,
                          output_to_es_raw=output_to_es_raw,
                          es_index=es_index,
                          doc_type=doc_type,
                          output_to_es_register=output_to_es_register)
            result_list.clear()

    def parse_video_page_multi_process(self, para_dic, processes_num=30):
        pool = Pool(processes=processes_num)
        for line in range(processes_num):
            pool.apply_async(self.parse_video_page_single_process, kwds=para_dic)
        pool.close()
        pool.join()

# test
if __name__=='__main__':
    test = Crawler_iqiyi()
    url = 'https://www.iqiyi.com/v_19rqsref08.html'
    video_dict = test.video_page(url)