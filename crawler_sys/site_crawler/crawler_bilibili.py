# -*- coding: utf-8 -*-
"""
Created on Wed Mar 28 14:13:32 2018

#cid 可以用来抓取弹幕
#search_page 抓不到 releaserurl


@author: fangyucheng
"""

import requests
import re
import json
import datetime
import time
import copy
from bs4 import BeautifulSoup
from crawler.crawler_sys.framework.video_fields_std import Std_fields_video
from crawler.crawler_sys.utils.output_results import bulk_write_into_es
from crawler.crawler_sys.utils.output_results import retry_get_url
from crawler.crawler_sys.utils.output_results import output_result
from crawler.crawler_sys.utils.trans_format import str_lst_to_file
from crawler.crawler_sys.proxy_pool.connect_with_database import extract_data_to_use
from crawler.crawler_sys.proxy_pool.connect_with_database import update_status
from crawler.crawler_sys.proxy_pool.build_proxy_dic import build_proxy_dic
from crawler.crawler_sys.utils.util_logging import logged


class Crawler_bilibili(Std_fields_video):

    def __init__(self, timeout=None, platform='bilibili'):
        if timeout==None:
            self.timeout = 10
        else:
            self.timeout = timeout
        self.platform = platform
        std_fields = Std_fields_video()
        self.video_data = std_fields.video_data
        self.video_data['platform'] = self.platform
        # remove fields that crawled data don't have
        pop_key_Lst = ['repost_count', 'isOriginal',]
        for popk in pop_key_Lst:
            self.video_data.pop(popk)

        self.lst_name_rid_dict = {'国产动画': '153',
                                  '搞笑': '138',
                                  '影视杂谈': '182',
                                  '纪录片': {'人文历史': '37',
                                             '科学探索自然': '178',
                                             '军事': '179',
                                             '社会美食旅行': '180'},
                                  '游戏': {'单机游戏': '17'}}

        self.headers = {'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Encoding':'gzip, deflate, br',
                        'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                        'Cache-Control':'max-age=0',
                        'Connection':'keep-alive',
                        'Cookie':'fts=1502427559; buvid3=E8FDA203-70E1-48A6-BE29-E2B833F92DB314456infoc; biliMzIsnew=1; biliMzTs=null; sid=534m3oqx; CNZZDATA2724999=cnzz_eid%3D1621908673-1502776053-https%253A%252F%252Fwww.baidu.com%252F%26ntime%3D1521001760; pgv_pvi=2734144512; rpdid=olilkosokkdoswsqmikqw; LIVE_BUVID=c552bf4415d1fba581d231647ba7b1bf; LIVE_BUVID__ckMd5=d8118a88b8f0fa8b; UM_distinctid=161e545d99a9-07c0b73fa83f93-17357940-1fa400-161e545d99b224; DedeUserID=114627314; DedeUserID__ckMd5=073268b15392f951; SESSDATA=4b30a63b%2C1524982595%2Cd78acc24; bili_jct=06e47d618fff20d978b968f15b3271c5; finger=c650951b; BANGUMI_SS_24014_REC=202051; _dfcaptcha=ca1c709bb04bda0240e4771eb8d90871',
                        'Host': 'www.bilibili.com',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:59.0) Gecko/20100101 Firefox/59.0'}


    def video_page(self, url):
        aid = url.split('av')[1]
        if len(aid) != 7:
            aid = aid[0:7]
        get_page = requests.get(url, headers=self.headers)
        get_page.encoding = 'utf-8'
        page = get_page.text
        soup = BeautifulSoup(page,'html.parser')
        title = soup.find('h1').text
        try:
            video_intro = soup.find('div',{'class':'info open'}).text
        except:
            video_intro = None
        try:
            releaser = soup.find('div',{'class':"user clearfix"}).a.text
            releaserUrl = soup.find('div',{'class':"user clearfix"}).a['href']
        except:
            releaser = None
            releaserUrl = None
        duration = re.findall(r'"duration":[0-9]{1,10}',
                              ' '.join(re.findall(r'videoData.*"duration":[0-9]{1,10}', page)))[0].split(':')[1]
        getcid = re.findall(r'cid=.*&aid='+aid,page)[0]
        cid = getcid.split('&')[0].split('=')[1]
        interfaceurl = ('https://interface.bilibili.com/player?id=cid:'
                        + cid + '&aid=' + aid)
        get_api_page = requests.get(interfaceurl)
        page = get_api_page.text
        soup = BeautifulSoup(page,'html.parser')
        play_count = soup.click.text
        release_time=soup.time.text
        fetch_time=int(datetime.datetime.timestamp(datetime.datetime.now()) * 1e3)
        D0 = copy.deepcopy(self.video_data)
        D0['title'] = title
        D0['play_count'] = play_count
        D0['releaser'] = releaser
        D0['fetch_time'] = fetch_time
        D0['describe'] = video_intro
        D0['release_time'] = release_time
        D0['duration'] = duration
        D0['releaserUrl'] = releaserUrl
        D0['url'] = url

        return D0


    def search_page(self, keyword, search_pages_max=30,
                    output_to_es_raw=False,
                    output_to_es_register=False,
                    es_index=None,
                    doc_type=None):
        search_lst = []
        def get_one_video(video_dict):
            title = video_dict['title']
            title = title.replace('<em class="keyword">', '')
            title = title.replace('</em>', '')
            aid = video_dict['aid']
            url = video_dict['arcurl']
            releaser = video_dict['author']
            video_intro = video_dict['description']
            dura = video_dict['duration']
            dura_lst = dura.split(':')
            duration = int(dura_lst[0])*60 + int(dura_lst[1])
            play_count = video_dict['play']
            comment_count = video_dict['video_review']
            favorite_count = video_dict['favorites']
            release_time = int(video_dict['pubdate'] * 1e3)
            tag = video_dict['tag']
            D0 = copy.deepcopy(self.video_data)
            D0['title'] = title
            D0['play_count'] = play_count
            D0['favorite_count'] = favorite_count
            D0['comment_count'] = comment_count
            D0['releaser'] = releaser
            D0['describe'] = video_intro
            D0['release_time'] = release_time
            D0['duration'] = duration
            D0['url'] = url

            return D0

        first_url = ('https://api.bilibili.com/x/web-interface/search/type?'
                     'jsonp=jsonp&search_type=video'
                     '&keyword=%s' % keyword)
        if search_pages_max == 1:
            search_urls = [first_url]
        else:
            search_gen = ('https://api.bilibili.com/x/web-interface/search/type?'
                          'jsonp=jsonp&search_type=video&keyword='
                          + keyword
                          + '&page={}'.format(
                              str(i)) for i in range(2, search_pages_max+1))
            search_urls = [first_url]
            search_urls.extend(list(search_gen))
        for s_url in search_urls:
            print(s_url)
            get_page = requests.get(s_url)
            page_dict = get_page.json()
            video_dicts = page_dict['data']['result']
            for video_dict in video_dicts:
                one_video_dict = get_one_video(video_dict)
                search_lst.append(one_video_dict)

                if len(search_lst) >= 100:
                    output_result(result_Lst=search_lst,
                                  platform=self.platform,
                                  output_to_es_raw=output_to_es_raw,
                                  output_to_es_register=output_to_es_register,
                                  es_index=es_index,
                                  doc_type=doc_type)
                    search_lst.clear()

        if search_lst != []:
            output_result(result_Lst=search_lst,
                          platform=self.platform,
                          output_to_es_raw=output_to_es_raw,
                          output_to_es_register=output_to_es_register,
                          es_index=es_index,
                          doc_type=doc_type)

        return search_lst


    def write_into_file(self, data_dict, file_obj):
        json_str=json.dumps(data_dict)
        file_obj.write(json_str)
        file_obj.write('\n')
        file_obj.flush()

    def feed_url_into_redis(self, dict_Lst):
        pass

    def output_result(self, result_Lst, output_to_file=False, filepath=None):
        # write data into es crawler-raw index
        bulk_write_into_es(result_Lst)

        # feed url into redis
        self.feed_url_into_redis(result_Lst)

        # output into file according to passed in parameters
        if output_to_file==True and filepath!=None:
            output_fn='crawler_toutiao_%s_json' % datetime.datetime.now().isoformat()[:10]
            output_f=open(filepath+'/'+output_fn, 'a', encoding='utf-8')
            self.write_into_file(result_Lst, output_f)
        else:
            pass


    def get_releaser_id(self,releaserUrl):
        rid = ' '.join(re.findall('[0-9]+',releaserUrl))
        return rid


    @logged
    def releaser_page(self, releaserUrl, release_time_upper_bdry=None,
                      release_time_lower_bdry=None, output_to_file=False, filepath=None):

        if release_time_upper_bdry == None:
            select_max = int((datetime.datetime.now()+datetime.timedelta(day=7)).timestamp()*1e3)
        else:
            select_max = int(datetime.datetime.strptime(release_time_upper_bdry,'%y-%m-%d').timestamp()*1e3)
        if release_time_lower_bdry == None:
            select_min = 0
        else:
            select_min = int(datetime.datetime.strptime(release_time_lower_bdry,'%y-%m-%d').timestamp()*1e3)

        rid = self.get_releaser_id(releaserUrl)
        result_Lst=[]
        count = 0
        firsturl='https://space.bilibili.com/ajax/member/getSubmitVideos?mid='+rid
        get_page = requests.get(firsturl)
        get_page.encoding = 'utf-8'
        page = get_page.text
        page = page.replace('false','False')
        page = page.replace('true','True')
        page = page.replace('null','None')
        page_dic = eval(page)
        totalvideo = int(page_dic['data']['count'])
        if int(totalvideo/20)==totalvideo/20:
            pagenum=int(totalvideo/20)
        else:
            pagenum=int(totalvideo/20)+1
        while count <= 30:
            for i in range(1,pagenum+1):
                url='https://space.bilibili.com/ajax/member/getSubmitVideos?mid='+rid+'&page='+str(i)
                get_page = requests.get(url)
                get_page.encoding = 'utf-8'
                page = get_page.text
                page = page.replace('false','False')
                page = page.replace('true','True')
                page = page.replace('null','None')
                page_dic = eval(page)
                video_dic = page_dic['data']['vlist']
                for one_video in video_dic:
                    release_time = one_video['created']*1e3
                    if release_time >= select_min and release_time < select_max:
                        title = one_video['title']
                        aid = one_video['aid']
                        url = 'https://www.bilibili.com/video/av'+str(aid)
                        releaser = one_video['author']
                        #video_intro = one_video['description']
                        dura = one_video['length']
                        dura_lst = dura.split(':')
                        duration = int(dura_lst[0])*60+int(dura_lst[1])
                        play_count = one_video['play']
                        comment_count = one_video['video_review']
                        favorite_count = one_video['favorites']
                        fetch_time = int(datetime.datetime.now().timestamp()*1e3)

                        self.video_data['platform']='bilibili'
                        self.video_data['title']=title
                        self.video_data['duration']=duration
                        self.video_data['url']=url
                        self.video_data['play_count']=play_count
                        self.video_data['comment_count']=comment_count
                        self.video_data['favorite_count']=favorite_count
                        self.video_data['video_id']=aid
                        self.video_data['releaser']=releaser
                        self.video_data['releaser_id']=int(rid)
                        self.video_data['release_time']=release_time
                        self.video_data['fetch_time']=fetch_time

                        print('get one video')
                        result_Lst.append(self.video_data)
                        if len(result_Lst)%100==0:
                            self.output_result(result_Lst,output_to_file=output_to_file,
                                               filepath=filepath)
                            result_Lst.clear()
                    elif release_time > select_max:
                        print('publish after requirement')
                    elif release_time <select_min:
                        count+=1
                        print('publish before requirement')
                self.output_result(result_Lst)


    def list_page(self, rid,
                  page_num=1,
                  channel=None,
                  output_to_file=False,
                  filepath=None,
                  output_to_es_raw=False,
                  output_to_es_register=False,
                  push_to_redis=False,
                  page_num_max=34,
                  output_es_index=None,
                  output_doc_type=None,
                  proxy_dic=None):

        result_lst = []
        fail_time = 0
        while page_num <= page_num_max and fail_time < 5:
            lst_url = ('https://api.bilibili.com/x/web-interface/newlist?rid='
                       + rid + '&type=0&pn=' + str(page_num) + '&ps=20')
            if proxy_dic is not None:
                raw_proxy_dic = extract_data_to_use()
                record_id = raw_proxy_dic['id']
                proxy_dic = build_proxy_dic(raw_proxy_dic=raw_proxy_dic)
                print('get proxy_dic %s' % proxy_dic)
            try:
                get_page = retry_get_url(lst_url, proxies=proxy_dic, timeout=15)
                fail_time = 0
                page_num += 1
            except:
                update_status(record_id=record_id,
                              availability=0)
                fail_time += 1
                print('%s has failed %s times' % (lst_url, fail_time))
                continue
            print('get page at %s' % (page_num-1))
            page_dic = get_page.json()
            total_video = int(page_dic['data']['page']['count'])

            if page_num == 1:
                if int(total_video/20) == total_video/20:
                    total_page_num = int(total_video/20)
                else:
                    total_page_num = int(total_video/20) + 1
                if total_page_num <= page_num_max:
                    page_num_max = total_page_num

            video_dic = page_dic['data']['archives']

            for one_video in video_dic:
                video_dic = copy.deepcopy(self.video_data)
                video_dic['title'] = one_video['title']
                aid = one_video['aid']
                video_dic['aid'] = one_video['aid']
                try:
                    attribute = one_video['attribute']
                except:
                    attribute=0
                video_dic['attribute'] = attribute
                video_dic['url'] = 'https://www.bilibili.com/video/av' + str(aid)
                video_dic['releaser'] = one_video['owner']['name']
                video_dic['releaser_id'] = one_video['owner']['mid']
                video_dic['video_intro'] = one_video['desc']
                video_dic['duration'] = one_video['duration']
                video_dic['play_count'] = one_video['stat']['view']
                video_dic['danmuku'] = one_video['stat']['danmaku']
                video_dic['release_time'] = (one_video['pubdate'])*1e3
                fetch_time=int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
                video_dic['fetch_time'] = fetch_time
                result_lst.append(video_dic)

                if len(result_lst) >= 100:
                    if output_es_index is None and output_doc_type is None:
                        output_result(result_Lst=result_lst,
                                      platform=self.platform,
                                      output_to_file=output_to_file,
                                      filepath=filepath,
                                      output_to_es_raw=output_to_es_raw,
                                      output_to_es_register=output_to_es_register,
                                      push_to_redis=push_to_redis)
                        result_lst.clear()

                    elif output_es_index is not None and output_doc_type is not None:
                        output_result(result_Lst=result_lst,
                                      platform=self.platform,
                                      output_to_file=output_to_file,
                                      filepath=filepath,
                                      output_to_es_raw=output_to_es_raw,
                                      output_to_es_register=output_to_es_register,
                                      push_to_redis=push_to_redis,
                                      es_index=output_es_index,
                                      doc_type=output_doc_type)
                        result_lst.clear()
            time.sleep(0)
        if result_lst != []:
            if output_es_index is None and output_doc_type is None:
                output_result(result_Lst=result_lst,
                              platform=self.platform,
                              output_to_file=output_to_file,
                              filepath=filepath,
                              output_to_es_raw=output_to_es_raw,
                              output_to_es_register=output_to_es_register,
                              push_to_redis=push_to_redis)

            elif output_es_index is not None and output_doc_type is not None:
                output_result(result_Lst=result_lst,
                              platform=self.platform,
                              output_to_file=output_to_file,
                              filepath=filepath,
                              output_to_es_raw=output_to_es_raw,
                              output_to_es_register=output_to_es_register,
                              push_to_redis=push_to_redis,
                              es_index=output_es_index,
                              doc_type=output_doc_type)

        return result_lst


    def fromdeathtoborn(self,rid,result_doc,done_doc,pn):
        list_page=[]
        result=open(result_doc,'a')
        done=open(done_doc,'a')
        firsturl='https://api.bilibili.com/x/web-interface/newlist?rid='+rid+'&type=0&pn=1&ps=20'
        get_page = requests.get(firsturl)
        get_page.encoding = 'utf-8'
        page = get_page.text
        page = page.replace('false','False')
        page = page.replace('true','True')
        page = page.replace('null','None')
        page_dic = eval(page)
        totalvideo = int(page_dic['data']['page']['count'])
        if int(totalvideo/20)==totalvideo/20:
            pagenum=int(totalvideo/20)
        else:
            pagenum=int(totalvideo/20)+1
        for i in range(pn,pagenum+1):
            url='https://api.bilibili.com/x/web-interface/newlist?rid='+rid+'&type=0&pn='+str(i)+'&ps=20'
            get_page = requests.get(url)
            get_page.encoding = 'utf-8'
            page = get_page.text
            page = page.replace('false','False')
            page = page.replace('true','True')
            page = page.replace('null','None')
            page_dic = eval(page)
            video_dic = page_dic['data']['archives']
            print(url)
            done.write(url)
            done.write('\n')
            done.flush()
            for one_video in video_dic:
                title = one_video['title']
                aid = one_video['aid']
                try:
                    attribute = one_video['attribute']
                except:
                    attribute=0
                url = 'https://www.bilibili.com/video/av'+str(aid)
                releaser = one_video['owner']['name']
                releaserid = one_video['owner']['mid']
                video_intro = one_video['desc']
                duration = one_video['duration']
                play_count = one_video['stat']['view']
                danmuku = one_video['stat']['danmaku']
                release_time = (one_video['pubdate'])*1e3
                fetch_time=int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
                D0={'title':title,'play_count':play_count,'releaser':releaser,'releaserid':releaserid,'video_intro':video_intro,'release_time':release_time,'duration':duration,'url':url,'aid':aid,'fetch_time':fetch_time,'subdomain':rid,'attribute':attribute,'danmuku':danmuku}
                json_dic=json.dumps(D0)
                result.write(json_dic)
                result.write('\n')
                list_page.append(D0)
                print('get one line')
            result.flush()
            time.sleep(3)
        result.close()
        done.close()
        return list_page

    def get_album(self,count,result_doc,done_doc):
        album_lst=[]
        result=open(result_doc,'a')
        done=open(done_doc,'a')
        while count<=26:
            url = ('https://bangumi.bilibili.com/media/web_api/search/result?season_type=3&page='+str(count)
                   +'&pagesize=20&style_id=0&order=0&sort=1&producer_id=0')
            get_page = requests.get(url)
            get_page.encoding = 'utf-8'
            page = get_page.text
            page = page.replace('false','False')
            page = page.replace('true','True')
            page = page.replace('null','None')
            page_dic = eval(page)['result']['data']
            print(count)
            count+=1
            done.write(str(count))
            done.write('\n')
            done.flush()
            for line in page_dic:
                album_name=line['title']
                album_infor=line['index_show']
                url=line['link']
                get_page = requests.get(url)
                get_page.encoding = 'utf-8'
                page = get_page.text
                soup = BeautifulSoup(page,'html.parser')
                try:
                    releaser = soup.find('div',{'class':"user clearfix"}).a.text
                    releaserurl = soup.find('div',{'class':"user clearfix"}).a['href']
                except:
                    releaser = 'bilibili纪录片'
                    releaserurl = 'https://space.bilibili.com/7584632'
                getid=re.findall(r'"epList".*"newestEp"',page)
                id_lst=getid[0]
                id_lst=id_lst.replace('"epList":','')
                id_lst=id_lst.replace(',"newestEp"','')
                id_lst=id_lst.replace('[','')
                id_lst=id_lst.replace(']','')
                id_lst=id_lst.replace('},{','},,,{')
                real_id_lst=id_lst.split(',,,')
                for line in real_id_lst:
                    try:
                        line=eval(line)
                        aid=line['aid']
                        cid=line['cid']
                        epid=line['ep_id']
                        title=line['index_title']
                        if title=='':
                            title=album_name
                        interfaceurl='https://interface.bilibili.com/player?id=cid:'+str(cid)+'&aid='+str(aid)
                        get_page2=requests.get(interfaceurl)
                        page2 = get_page2.text
                        soup2 = BeautifulSoup(page2,'html.parser')
                        play_count=soup2.click.text
                        release_time=soup2.time.text
                        release_time=int(release_time*1e3)
                        fetch_time=int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
                        dura=soup2.duration.text
                        dura_lst=dura.split(':')
                        if len(dura_lst)==1:
                            duration=int(dura_lst[0])
                        elif len(dura_lst)==2:
                            duration=int(dura_lst[0])*60+int(dura_lst[1])
                        elif len(dura_lst)==3:
                            duration=int(dura_lst[0])*3600+int(dura_lst[1])*60+int(dura_lst[2])
                        else:
                            duration=0
                        url='https://www.bilibili.com/bangumi/play/ep'+str(epid)
                        D0={'album_name':album_name,'album_infor':album_infor,'title':title,
                            'play_count':play_count,'releaser':releaser,'fetch_time':fetch_time,
                            'release_time':release_time,'duration':duration,'url':url,
                            'releaserurl':releaserurl,'aid':aid,'cid':cid}
                        album_lst.append(D0)
                        json_D0=json.dumps(D0)
                        result.write(json_D0)
                        result.write('\n')
                        print(url)
                    except:
                        print('thereisanerror'+url)
                result.flush()
        result.close()
        done.close()
        return album_lst

    def renew_playcount(self,taskname,resultname):
        result=open(resultname,'a')
        task=open(taskname)
        task_lst=[]
        result_lst=[]
        for line in task:
            line_dic=eval(line)
            task_lst.append(line_dic)
        for line in task_lst:
            aid=line['aid']
            cid=line['cid']
            interfaceurl='https://interface.bilibili.com/player?id=cid:'+str(cid)+'&aid='+str(aid)
            get_page=requests.get(interfaceurl)
            page = get_page.text
            soup = BeautifulSoup(page,'html.parser')
            play_count=soup.click.text
            fetch_time=int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
            line['play_count']=play_count
            line['fetch_time']=fetch_time
            result_lst.append(line)
            json_line=json.dumps(line)
            result.write(json_line)
            result.write('\n')
            result.flush()
            print(aid)
        task.close()
        result.close()
        return result_lst

    def get_video_danmuku(self, url):
        result_lst = []
        video_dic = self.video_page(url)
        cid = video_dic['cid']
        danmu_url = 'https://comment.bilibili.com/%s.xml' % cid
        get_page = requests.get(danmu_url)
        get_page.encoding = "utf-8"
        page = get_page.text
        soup = BeautifulSoup(page,'html.parser')
        danmu_lst = soup.find_all("d")
        for line in danmu_lst:
            danmu = line.text
            result_lst.append(danmu)
        return result_lst


if __name__=='__main__':
    test = Crawler_bilibili()
    # task_lst = ["https://www.bilibili.com/video/av9062434",
    #             "https://www.bilibili.com/video/av7934136",
    #             "https://www.bilibili.com/video/av8882049",
    #             "https://www.bilibili.com/video/av7495100"]
    # for url in task_lst:
    #     aid = url.split('av')[1]
    #     result = test.get_video_danmuku(url)
    #     str_lst_to_file(listname=result, filename=aid)
    #video_dic = test.video_page("https://www.bilibili.com/video/av9062434")
    sr_bb = test.search_page(keyword='任正非 BBC', search_pages_max=3)
"""
https://www.bilibili.com/video/av9062434
https://www.bilibili.com/video/av7934136
https://www.bilibili.com/video/av8882049
https://www.bilibili.com/video/av7495100
"""


