# -*- coding: utf-8 -*-
"""
Created on Mon Mar 19 10:32:52 2018

@author: fangyucheng
"""


import requests
from bs4 import BeautifulSoup
import json


#两会：lh/_cl/0/30/
#访谈：ft/_cl/6/30/
#资讯：zx/_cl/29/20/
#视点：sd/_cl/29/20/
#全球：qq/_cl/7/30/

class people_video():
   
    def video_page(self,url):
        get_page=requests.get(url)
        get_page.encoding='utf-8'
        page = get_page.text
        midstep1=json.loads(page)
        midstep2=midstep1['data']['article']
        midstep3=midstep2['publish']
        title=midstep2['title']
        author=midstep2['author']
        release_time=midstep2['publishTime']
        playcount=midstep2['playNum']
        dura=midstep2['duration']
        duration_str=dura
        dl=duration_str.split(':')
        dl_int=[]
        for v in dl:
            v=int(v)    
            dl_int.append(v) 
        if len(dl_int) == 2:
            duration=dl_int[0]*60+dl_int[1]
        else:
            duration=dl_int[0]*3660+dl_int[1]*60+dl_int[2]
        releaser=midstep3['name']
        D0={'title':title,'playcount':playcount,'releaser':releaser,'release_time':release_time,'duration':duration,'author':author,'url':url}
        return D0   
    
    def list_page(self,partofurl,totalpage):
        urls=['http://mobilevideo.people.com.cn/movie_pub/News/publishfile/'+partofurl+'list_{}.json'.format(str(i)) for i in range(1,totalpage)]
        list_page=[]
        for url in urls:
            get_page=requests.get(url)
            get_page.encoding='utf-8'
            page=get_page.text
            soup=BeautifulSoup(page,'html.parser')
            try:
                selection=soup.html.head.title.text
                print ('no more page')
            except AttributeError:
                print(url)
                midstep1=json.loads(page)
                midstep2=midstep1['data']['newsList']
                for one_line in midstep2:
                    url=one_line['articleLink']
                    one_video_dic=self.video_page(url)
                    list_page.append(one_video_dic)
        return list_page
            
    
    
if __name__=='__main__':
    people_crawler = people_video()
    #video_page=people_video.video_page(url='http://mobilevideo.people.com.cn/movie_pub/News/publishfile/spk/_cd/10/18/4154954.json')
    list_page2=people_crawler.list_page(partofurl="qq/_cl/7/30/",totalpage=20)
    #search_page=iqiyi_crawler.search_page(keyword="国家相册")
    