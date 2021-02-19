# -*- coding: utf-8 -*-
"""
Created on Tue Apr 10 17:52:08 2018

@author: fangyucheng
"""


import datetime
import json
import requests
from bs4 import BeautifulSoup

def get_video(target):
    result = []
    count = 0
    while len(result) < target and count < 100:
        listurl = 'http://apis.tudou.com/homepage/v2/index/get_push.json?secCateId=622736331'
        get_page = requests.get(listurl)
        get_page.encoding = 'utf-8'
        page = get_page.text
        print('get one page')
        page = page.replace('true', 'True')
        page = page.replace('false', 'False')
        page_dic = json.loads(page)['entity']
        for line in page_dic:
            midstep = line['detail']
            title = midstep['base_detail']['title']
            playcount = midstep['video_detail']['vv_desc']
            releaser = midstep['user_detail']['name']
            releaserid = midstep['user_detail']['id']
            videoid = midstep['video_detail']['video_id']
            duration = midstep['video_detail']['duration']
            url = 'http://new-play.tudou.com/v/'+videoid
            get_page = requests.get(url)
            page = get_page.text
            soup = BeautifulSoup(page, 'html.parser')
            rt_step1 = soup.find('div', {'class':'td-play__videoinfo__details-box__time'})
            rt_step2 = rt_step1.text[:-2]
            release_time = int(datetime.datetime.strptime(rt_step2,
                                                          '%Y-%m-%d %H:%M:%S').timestamp()*1e3)
            D0 = {"title":title, "releaser":releaser, "release_time":release_time,
                  "duration":duration, 'releaserid':releaserid, 'playcount':playcount}
            if D0 not in result:
                result.append(D0)
                print('added one video')
            else:
                count += 1
                print('repetition')
    return result



if __name__=='__main__':
    try1 = get_video(target=200)
    #{'旅行':'http://apis.tudou.com/homepage/v2/index/get_push.json?secCateId=10293',
    #'科技':'http://apis.tudou.com/homepage/v2/index/get_push.json?secCateId=10199',
    #'娱乐':'http://apis.tudou.com/homepage/v2/index/get_push.json?secCateId=622726317',
    #'萌物':'http://apis.tudou.com/homepage/v2/index/get_push.json?secCateId=622485153'}
