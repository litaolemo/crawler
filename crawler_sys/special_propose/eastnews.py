# -*- coding: utf-8 -*-
"""
Created on Tue Mar 13 15:21:47 2018

@author: fangyucheng
"""

import requests
from bs4 import BeautifulSoup
import re
import datetime
import pickle
import pandas as pd 

class Crawler_v_qq_eastnews:
    
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
        except:
            releaser=None
        try:
            releaserUrl=soup.find('a',{'class':'user_info'})['href']
        except TypeError:
            releaserUrl=None
        try:
            video_intro=soup.find('meta',{'itemprop':'description'})['content']
        except TypeError:
            video_intro=None
        try:
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
        except:
            duration=None
            playcount=None
            release_time=None
        D0={'title':title,'playcount':playcount,'releaser':releaser,'video_intro':video_intro,'release_time':release_time,'duration':duration,'releaserUrl':releaserUrl}
        return D0  
     
    def search_page(self,totalpage):
        video_Lst=[]
        url_Lst=[]
        page_Lst=['https://v.qq.com/x/search/?ses=qid%3D_5hveCy5oWKS_b5d4GuLquXTO29F8LJnLcmNDpNkXFkeEr8UDB0g9g%26last_query%3D%E4%B8%9C%E6%96%B9%E6%96%B0%E9%97%BB%26tabid_list%3D0%7C11%7C8%7C7%26tabname_list%3D%E5%85%A8%E9%83%A8%7C%E6%96%B0%E9%97%BB%7C%E5%8E%9F%E5%88%9B%7C%E5%85%B6%E4%BB%96&q=%E4%B8%9C%E6%96%B9%E6%96%B0%E9%97%BB&stag=3&cur={}&cxt=tabid%3D0%26sort%3D1%26pubfilter%3D0%26duration%3D3'.format(str(i)) for i in range(1,totalpage)]
        for page_url in page_Lst:
            get_page=requests.get(page_url)
            print (page_url)
            get_page.encoding='utf-8'
            page = get_page.text
            soup = BeautifulSoup(page,'html.parser')
            tencent = soup.find_all("div", { "class" : "result_item result_item_h _quickopen" })
            for data_line in tencent:
                try:
                    ttt=data_line.find('span',{'title':'东方新闻'}).text
                except AttributeError:
                    ttt=None
                if ttt==None:
                    urls=None
                else:
                    urls=data_line.h2.a['href']
                    get_page=requests.get(urls)
                    print (urls)
                    get_page.encoding='utf-8'
                    page = get_page.text
                    soup = BeautifulSoup(page,'html.parser')
                    fff=soup.find_all('a',{'class':'figure_detail'})      
                    for zzz in fff:
                        urls1=zzz['href']
                        urls2='https://v.qq.com'+urls1
                        url_Lst.append(urls2)
        for url in url_Lst:
            dicdic = self.video_page(url)
            dicdic['url']=url
            print(url)
            video_Lst.append(dicdic)
        return video_Lst
  

if __name__=='__main__':
    v_qq_crawler = Crawler_v_qq_eastnews()
    search_page2=v_qq_crawler.search_page(totalpage=20)   