# -*- coding: utf-8 -*-
"""
Created on Tue Mar 13 12:16:46 2018

@author: fangyucheng
"""


import requests
from bs4 import BeautifulSoup
import re
import datetime
import pickle
import pandas as pd  

class Crawler_v_qq_seeeast:
    
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
            release_time=int(datetime.datetime.strptime(retime,'%Y-%m-%d').timestamp()*1e3)
        except AttributeError:
            duration=0
            playcount=0
            release_time=0
        D0={'title':title,'playcount':playcount,'releaser':releaser,'video_intro':video_intro,'release_time':release_time,'duration':duration,'releaserUrl':releaserUrl}
        return D0  
     
    def search_page(self,totalpage):
        video_Lst=[]
        url_Lst=[]
        page_Lst=['https://v.qq.com/x/search/?ses=qid%3D_tWkHF0wGSaxTrPo1x9xd0NnsAZaHm3ts4uGEB7pLLeyHMZ1YQ-myg%26last_query%3D%E7%9C%8B%E4%B8%9C%E6%96%B9%26tabid_list%3D0%7C3%7C11%7C12%7C8%26tabname_list%3D%E5%85%A8%E9%83%A8%7C%E7%BB%BC%E8%89%BA%7C%E6%96%B0%E9%97%BB%7C%E5%A8%B1%E4%B9%90%7C%E5%8E%9F%E5%88%9B&q=%E7%9C%8B%E4%B8%9C%E6%96%B9&stag=3&cur={}&cxt=tabid%3D0%26sort%3D1%26pubfilter%3D0%26duration%3D3'.format(str(i)) for i in range(1,totalpage)]
        for page_url in page_Lst:
            get_page=requests.get(page_url)
            print (page_url)
            get_page.encoding='utf-8'
            page = get_page.text
            soup = BeautifulSoup(page,'html.parser')
            tencent = soup.find_all("div", { "class" : "result_item result_item_h _quickopen" })
            for data_line in tencent:
                stepone=data_line.find('div',{'class':'result_info'}).div.div
                steptwo=stepone.find('span',{'class':'content'}).text
                stepthree=steptwo.split('-')[1]
                if stepthree=='02':
                    try:
                        ttt=data_line.find('span',{'title':'看东方'}).text
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
    v_qq_crawler = Crawler_v_qq_seeeast()
    #video_data=v_qq_crawler.search_page(keyword='国家相册')
    search_page=v_qq_crawler.search_page(totalpage=5)
    
    
    
    #eastnewFeb=pd.DataFrame(eastnewsFeb)
    #eastnewFeb.to_csv('东方新闻-二月',encoding='gb18030',index=False)
    #eastnewFeb['release_time_H']=eastnewFeb['release_time']-4*3600*1e3
    #eastnewFeb['real_time']=pd.to_datetime(eastnewFeb['release_time_H'],unit='ms')
    #counter=1
    #for url in url_Lst:
        #dicdic = self.video_page(url)
        #dicdic['url']=url
        #video_Lst.append(dicdic)
        #counter+=1
        #print(counter)
        #if counter%1000==0:
            #with open('seeeast_data_Lst_pickle', 'ab') as pickleF:
              #pickle.dump(video_Lst, pickleF)  
              #video_Lst.clear
    #if len(video_Lst)>0:
        #with open('seeeast_data_Lst_pickle', 'ab') as pickleF:
              #pickle.dump(video_Lst, pickleF)
   seeeast=pd.DataFrame(search_page)

seeeast['re']=seeeast['release_time']-4*3600*1e3

seeeast['real_time']=pd.to_datetime(seeeast['re'],unit='ms')

seeeast.to_csv('东方新闻xin-二月',encoding='gb18030',index=False)