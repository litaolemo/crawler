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

import requests
from bs4 import BeautifulSoup
import datetime
import re
import pandas as pd
import json




class Crawler_v_qq:
    
    def video_page(self, url):
        get_page=requests.get(url)
        get_page.encoding='utf-8'
        page = get_page.text
        soup = BeautifulSoup(page,'html.parser')     
        try:
            title=soup.find('h1',{'class':'video_title _video_title'}).text
            title=title.replace('\n','')
            title=title.replace('\t','')
        except AttributeError:
            title=None
        try:
            releaser=soup.find('span',{'class':'user_name'}).text
        except AttributeError:
            releaser=None
        try:
            releaserUrl=soup.find('a',{'class':'user_info'})['href']
        except TypeError:
            releaserUrl=None
        try:
            video_intro=soup.find('meta',{'itemprop':'description'})['content']
        except TypeError:
            video_intro=None
        midstep = soup.find("script",{"r-notemplate":"true"}).text
        try:
            duration = re.findall(r'"duration":[0-9]{1,10}', ','.join(re.findall(r'VIDEO_INFO.*"duration":[0-9]{1,10}', midstep)))[0].split(':')[1]
        except IndexError:
            duration = re.findall(r'"duration":"[0-9]{1,10}"', ','.join(re.findall(r'VIDEO_INFO.*"duration":"[0-9]{1,10}"', midstep)))[0].split(':')[1]
            duration=duration.replace('"','')
            duration=int(duration)
        except:
            print('Catched exception, didn\'t find duartion in var VIDEO_INFO')
            duration=0
        try:
            playcount = re.findall(r'"view_all_count":[0-9]{1,10}', ','.join(re.findall(r'VIDEO_INFO.*"view_all_count":[0-9]{1,10}', midstep)))[0].split(':')[1]
        except:
            print('Catched exception, didn\'t find view_all_count in var VIDEO_INFO')
            playcount=0
        retime=re.findall(r'"video_checkup_time":"\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d"', ','.join(re.findall(r'VIDEO_INFO.*"video_checkup_time":"\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d"', midstep)))[0].split('":"')[1].split(' ')[0]
        try:
            release_time=int(datetime.datetime.strptime(retime,'%Y-%m-%d').timestamp()*1e3)
        except ValueError:
            release_time=0
        fetch_time=int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
        try:
            target=soup.find('div',{'class':'video_tags _video_tags'}).text
        except:
            target=None
        D0={'title':title,'target':target,'play_count':playcount,'releaser':releaser,'video_intro':video_intro,'release_time':release_time,'duration':duration,'releaserUrl':releaserUrl,'url':url,'fetch_time':fetch_time}
        return D0   
    
    
    def search_page(self, keyword):
        search_page_Lst=[]
        def process_one_line(data_line):
            url=data_line.h2.a['href']
            dicdicdic=self.video_page(url)
            return dicdicdic
        search_url = ['https://v.qq.com/x/search?q='+keyword+'&cur={}'.format(str(i)) for i in range(1,6)]
        for urls in search_url:
            get_page=requests.get(urls)
            print(urls)
            get_page.encoding='utf-8'
            page=get_page.text
            soup = BeautifulSoup(page,'html.parser')
            tencent = soup.find_all("div", { "class" : "result_item result_item_h _quickopen" })
            for data_line in tencent:
                one_line_dic=process_one_line(data_line)
                print('get one line done')
                search_page_Lst.append(one_line_dic)
        return search_page_Lst
    
    def releaser_page(self, releaserurl):
        releaser_data_Lst=[]
        get_page=requests.get(releaserurl)
        get_page.encoding='utf-8'
        page=get_page.text
        soup = BeautifulSoup(page,'html.parser')
        totalvideonum=int(soup.find('span',{'class':'count_num'}).text[1:-3])
        if totalvideonum/24==int(totalvideonum/24):
            totalvideopage=int(totalvideonum/24)
        else:
            totalvideopage=int(totalvideonum/24)+1
        releaserID=soup.find('span',{'class':'sns_btn'}).a['data-vuin']
        video_page_url = ['http://c.v.qq.com/vchannelinfo?otype=json&uin='+releaserID+'&qm=1&pagenum={}&num=24'.format(str(i)) for i in range(1,totalvideopage)]
        for urls in video_page_url:
            get_page=requests.get(urls)
            print(urls)
            get_page.encoding='utf-8'
            page=get_page.text
            twenty_four_video=page.split('[')[1].split(']')[0].replace('},{','},,,{').split(',,,')
            for a_video in twenty_four_video:
                url=eval(a_video)['url']
                one_video_dic=self.video_page(url)
                print('get one line done')
                releaser_data_Lst.append(one_video_dic)
        return releaser_data_Lst
    
    
    
    def list_page(self, listurl):
        #listurl=http://v.qq.com/x/list/fashion/
        list_data_Lst=[]
        listnum=[]
        for i in range(0,34):
            list_num=i*30
            listnum.append(list_num)
        #最近热播
        listpage=[listurl+'?&offset={}'.format(str(i)) for i in listnum]
        #最近上架
        #listpage=[listurl+'?sort=5&offset={}'.format(str(i)) for i in listnum]
        for listurls in listpage:
            get_page=requests.get(listurls)
            get_page.encoding='utf-8'
            page = get_page.text
            print(listurls)
            soup = BeautifulSoup(page,'html.parser')
            midstep=soup.find_all('li',{'class':'list_item'})
            counter=0
            for urls in midstep:
                url=urls.a['href']
                one_video_dic=self.video_page(url)
                counter+=1
                print(counter)
                list_data_Lst.append(one_video_dic)
        return list_data_Lst
    
    
    def doc_list_page(self, listurl):
        #listurl=http://v.qq.com/x/list/fashion/
        done=open('done_qq','a')
        result=open('result_qq','a')
        error=open('error_qq','a')
        list_data_Lst=[]
        listnum=[]
        for i in range(0,93):
            list_num=i*30
            listnum.append(list_num)
        #最近热播
        listpage=[listurl+'?&offset={}'.format(str(i)) for i in listnum]
        #最近上架
        #listpage=[listurl+'?sort=5&offset={}'.format(str(i)) for i in listnum]
        for listurl in listpage:
            get_page=requests.get(listurl)
            get_page.encoding='utf-8'
            page = get_page.text
            print(listurl)
            done.write(listurl)
            done.write('\n')
            done.flush()
            soup = BeautifulSoup(page,'html.parser')
            midstep=soup.find_all('strong',{'class':'figure_title'})
            for line in midstep:
                album_name=line.text
                url=line.a['href']
                get_page=requests.get(url)
                get_page.encoding='utf-8'
                page = get_page.text
                soup = BeautifulSoup(page,'html.parser')    
                try:
                    get_all_url=soup.find('ul',{'class':'figure_list _hot_wrapper'})
                    url_agg=get_all_url.find_all('a',{'class':'figure_detail'})
                    urllist=[]
                    for line in url_agg:
                        url_part=line['href']
                        url='https://v.qq.com'+url_part
                        urllist.append(url)
                    for url in urllist:
                        try:
                            one_video=self.video_page(url)
                            one_video['album_name']=album_name
                            print(url)
                            list_data_Lst.append(one_video)
                            one_video_json=json.dumps(one_video)
                            result.write(one_video_json)
                            result.write('\n')
                            result.flush()
                        except AttributeError:
                            D0={'url':url,'album_name':album_name}
                            print('there is an error')
                            json_D0=json.dumps(D0)
                            error.write(json_D0)
                            error.write('\n')
                            error.flush()
                except:
                   one_video=self.video_page(url)
                   one_video['album_name']=album_name
                   print(url)
                   list_data_Lst.append(one_video)
                   one_video_json=json.dumps(one_video)
                   result.write(one_video_json)
                   result.write('\n')
                   result.flush()
        done.close()  
        result.close()
        error.close()
        return list_data_Lst

    def doc_list_reborn(self, listurl,x):
        #listurl=http://v.qq.com/x/list/fashion/
        done=open('done_qq','a')
        result=open('result_qq','a')
        error=open('error_qq','a')
        list_data_Lst=[]
        listnum=[]
        for i in range(x,93):
            list_num=i*30
            listnum.append(list_num)
        #最近热播
        listpage=[listurl+'?&offset={}'.format(str(i)) for i in listnum]
        #最近上架
        #listpage=[listurl+'?sort=5&offset={}'.format(str(i)) for i in listnum]
        for listurl in listpage:
            get_page=requests.get(listurl)
            get_page.encoding='utf-8'
            page = get_page.text
            print(listurl)
            done.write(listurl)
            done.write('\n')
            done.flush()
            soup = BeautifulSoup(page,'html.parser')
            midstep=soup.find_all('strong',{'class':'figure_title'})
            for line in midstep:
                album_name=line.text
                url=line.a['href']
                get_page=requests.get(url)
                get_page.encoding='utf-8'
                page = get_page.text
                soup = BeautifulSoup(page,'html.parser')    
                try:
                    get_all_url=soup.find('ul',{'class':'figure_list _hot_wrapper'})
                    url_agg=get_all_url.find_all('a',{'class':'figure_detail'})
                    urllist=[]
                    for line in url_agg:
                        url_part=line['href']
                        url='https://v.qq.com'+url_part
                        urllist.append(url)
                    for url in urllist:
                        try:
                            one_video=self.video_page(url)
                            one_video['album_name']=album_name
                            print(url)
                            list_data_Lst.append(one_video)
                            one_video_json=json.dumps(one_video)
                            result.write(one_video_json)
                            result.write('\n')
                            result.flush()
                        except:
                            D0={'url':url,'album_name':album_name}
                            print('there is an error')
                            json_D0=json.dumps(D0)
                            error.write(json_D0)
                            error.write('\n')
                            error.flush()
                except:
                    try:
                        one_video=self.video_page(url)
                        one_video['album_name']=album_name
                        print(url)
                        list_data_Lst.append(one_video)
                        one_video_json=json.dumps(one_video)
                        result.write(one_video_json)
                        result.write('\n')
                        result.flush()
                    except:
                        D0={'url':url,'album_name':album_name}
                        print('there is an error')
                        json_D0=json.dumps(D0)
                        error.write(json_D0)
                        error.write('\n')
                        error.flush()
        done.close()  
        result.close()
        error.close()
        return list_data_Lst        
    
# test
if __name__=='__main__':
    v_qq_crawler = Crawler_v_qq()
    #video_data2=v_qq_crawler.search_page(keyword='国家相册')
    result=open('result_qq','a')
    for line in urllist:
        try:
            album_name=['album_name']
            url=line['url']
            one_video=v_qq_crawler.video_page(url)
            print('get one video')
            one_video['album_name']=album_name
            one_video_json=json.dumps(one_video)
            result.write(one_video_json)
            result.write('\n')
            result.flush()
        except:
            print(url)
        
   # video_data3=v_qq_crawler.releaser_page(releaserurl='http://v.qq.com/vplus/xhpmtt/videos')
    #x=int(2760/30)
    #listurl='http://v.qq.com/x/list/doco'
   # video_data4=v_qq_crawler.doc_list_reborn(listurl,x)    
#    list_page_url='https://v.qq.com/finance'
#    list_page_data_Lst=v_qq_crawler.list_page(list_page_url)




