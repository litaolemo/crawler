# -*- coding: utf-8 -*-
"""
Created on Tue Apr 10 11:06:53 2018

@author: fangyucheng
"""

import requests
import datetime



class crawler_pepper():
    
    def except_followers(self):
        rank=[]
        urls=['http://webh.huajiao.com/rank/recv?&type=day','http://webh.huajiao.com/rank/recv?&type=week','http://webh.huajiao.com/rank/recv?&type=all','http://webh.huajiao.com/rank/sun?&type=day','http://webh.huajiao.com/rank/sun?&type=week','http://webh.huajiao.com/rank/sun?&type=all','http://webh.huajiao.com/rank/user?&type=day','http://webh.huajiao.com/rank/user?&type=all','http://webh.huajiao.com/rank/send?&type=day','http://webh.huajiao.com/rank/send?&type=week','http://webh.huajiao.com/rank/send?&type=all']
        for url in urls:
            get_page=requests.get(url)
            get_page.encoding='utf-8'
            page = get_page.text
            print(url)
            page_dic = eval(page)['data']['rank']
            one_rank=[]
            for one_video in page_dic:
                category=url[29:33]
                timespan=url[-3:]
                name=one_video['nickname']
                authorlevel=one_video['authorlevel']
                level=one_video['level']
                try:
                    score=one_video['score']
                except KeyError: 
                    score=None
                uid=one_video['uid']
                currenttime=datetime.datetime.timestamp(datetime.datetime.now())*1e3
                D0={'name':name,'level':level,'authorlevel':authorlevel,'score':score,'uid':uid,'category':category,'timespan':timespan,'acttime':currenttime}
                one_rank.append(D0)
            rank.append(one_rank)
        return rank
    
    def get_followers(self):
        rank=[]
        urls=['http://webh.huajiao.com/rank/followers?&type=day','http://webh.huajiao.com/rank/followers?&type=all']
        for url in urls:
            get_page=requests.get(url)
            get_page.encoding='utf-8'
            page = get_page.text
            print(url)
            midstep = eval(page)
            page_dic = midstep['data']['rank']
            one_rank=[]
            for one_video in page_dic:
                category='followers'
                timespan=url[-3:]
                name=one_video['nickname']
                authorlevel=one_video['authorlevel']
                level=one_video['level']
                followers=one_video['followers']
                uid=one_video['uid']
                currenttime=datetime.datetime.timestamp(datetime.datetime.now())*1e3
                D0={'name':name,'level':level,'authorlevel':authorlevel,'followers':followers,'uid':uid,'category':category,'timespan':timespan,'acttime':currenttime}
                one_rank.append(D0)
            rank.append(one_rank)
        return rank
    
    
if __name__=='__main__':
    ttt =crawler_pepper()
    followers=ttt.get_followers() 
    others=ttt.except_followers()