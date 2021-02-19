# -*- coding: utf-8 -*-
"""
Created on Wed Aug 22 10:57:16 2018

@author: fangyucheng
"""


import re
import time
import json
import requests
from bs4 import BeautifulSoup
from crawler_sys.utils.trans_strtime_to_timestamp import trans_strtime_to_timestamp

cookie = ('sm_uuid=300c859169f7a0e16f6f3fd637b51b44%7C%7C%7C1534912939;'
          'sm_diu=300c859169f7a0e16f6f3fd637b51b44%7C%7C11eeeeee4a6df8bafe%7C1534912939;'
          'cna=T8v9EQPOimUCAXL/Az0YrDOB;'
          'isg=BEpKJ2iKJQYvZqlV7VhJwkckmDMsk__fIdGRvNSD9x0oh-tBvMtipV61kzX-bEYt;'
          'sm_sid=9a1582ab658abd059600560bb5d855a0;'
          'phid=9a1582ab658abd059600560bb5d855a0')


headers = {'Host': 'api.m.sm.cn',
           'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 10_2_1 like Mac OS X) AppleWebKit/602.4.6 (KHTML, like Gecko) Version/10.0 Mobile/14D27 Safari/602.1',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
           'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
           'Accept-Encoding': 'gzip, deflate, br',
           'Cookie': cookie,
           'Connection': 'keep-alive',
           'Upgrade-Insecure-Requests': '1',
           'Cache-Control': 'max-age=0'}


video_page_cookie = ('Hm_lvt_c337010bc5a1154d2fb6741a4d77d226=1535364629;'
               'Hm_lpvt_c337010bc5a1154d2fb6741a4d77d226=1535364629;'
               'vpstoken=t7Kp5v8ulKpE3VrNXYWg6w%3D%3D;'
               'cna=T8v9EQPOimUCAXL/Az0YrDOB; hasLoadCommentEmojiData=1;'
               'isg=BOjoR1oOJwg5eguMtQXfZ9aDutU6uX1RiONAHqIZNGNW_YhnSiEcq34_8RUNVgTz;'
               '_pk_id.070b5f1f4053.1564=84fc2996-3cae-4f2a-8df4-92f03a3ce790.1535371022.1.1535371040.1535371022.;'
               '_pk_ses.070b5f1f4053.1564=*')


video_page_headers = {'Host': 'mparticle.uc.cn',
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:61.0) Gecko/20100101 Firefox/61.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                'Accept-Encoding': 'gzip, deflate, br',
                'Cookie': video_page_cookie,
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0'}


other_type_name_cookie = ('sm_uuid=300c859169f7a0e16f6f3fd637b51b44%7C%7C%7C1534912939;'
                          'sm_diu=300c859169f7a0e16f6f3fd637b51b44%7C%7C11eeeeee4a6df8bafe%7C1534912939;'
                          'cna=T8v9EQPOimUCAXL/Az0YrDOB;'
                          'isg=BKSkG1vmo5TKrNcnhx6PEP2KdqFWlfntaw-Pbr7FMW94aUUz5k2-NwSDLQdUqgD_;'
                          'sm_sid=254b6b0e0ceded0fc0e605dd15979af4;'
                          'phid=254b6b0e0ceded0fc0e605dd15979af4')


other_type_name_headers = {'Host': 'm.sm.cn',
                           'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:61.0) Gecko/20100101 Firefox/61.0',
                           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                           'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                           'Accept-Encoding': 'gzip, deflate, br',
                           'Cookie': other_type_name_cookie,
                           'Connection': 'keep-alive',
                           'Upgrade-Insecure-Requests': '1',
                           'Cache-Control': 'max-age=0'}


keyword_type_dic = {'足球': 'Football',
                    'NBA': 'Nbanew',
                    '精选头条': 'Highquality.quality',}


def get_whole_page_from_article_url(url, whole_page_text):
    biz_id_str = re.findall('"biz_org_id":\d+,', whole_page_text)[0]
    biz_id = re.findall('\d+', biz_id_str)[0]
    get_article_id_lst = url.split('!')
    for line in get_article_id_lst:
        if 'wm_aid' in line:
            get_aid = line
    article_id = get_aid.split('=')[-1]
    article_url = 'https://ff.dayu.com/contents/origin/' + article_id + '?biz_id=' + str(biz_id)
    get_article_page = requests.get(article_url)
    get_article_page_dic = get_article_page.json()
    whole_page_text = get_article_page_dic['body']['text']
    print('get whole page from %s' % article_url)
    return whole_page_text



def shenma_info_page_highquality(keyword, max_page_num, kw_type='精选头条'):
    keyword_type = keyword_type_dic[kw_type]
    page_num = 1
    result_lst = []
    while page_num <= max_page_num:
        search_url = 'https://api.m.sm.cn/rest?q=' + keyword +'&method=' + keyword_type +'&page_num=' + str(page_num)
        get_page = requests.get(search_url, headers=headers)
        page_dic = get_page.json()
        if page_dic['error'] == 1:
            print('there is no content about %s' % keyword)
            return result_lst
        info_lst = page_dic['data']['content']
        print('get page at page %s' % page_num)
        page_num += 1
        for line in info_lst:
            title = line['title']
            title = title.replace('<em>', '').replace('</em>', '')
            url = line['url']
            releaser_time_str = line['time']
            release_time = trans_strtime_to_timestamp(releaser_time_str)
            source = line['source']
            try:
                get_whole_page = requests.get(url, headers=video_page_headers)
                whole_page_text = get_whole_page.text
                try:
                    whole_page_text = get_whole_page_from_article_url(url, whole_page_text)
                except:
                    print("don't get article detail from dayu")
            except:
                print('get whole page process error %s' % title)
                whole_page_text = 'missing'
            info_dic = {'title': title,
                        'url': url,
                        'release_time': release_time,
                        'source': source,
                        'whole_page_text': whole_page_text}
            result_lst.append(info_dic)
    return result_lst


def shenma_info_page_other_type_name(keyword, max_page_num, kw_type='NBA'):
    keyword_type = keyword_type_dic[kw_type]
    page_num = 1
    result_lst = []
    while page_num <= max_page_num:
        search_url = ('https://m.sm.cn/api/rest?method=' + keyword_type + '.feed&format=json&q='
                      + keyword + '&uads=&page=' + str(page_num)) + '&ps_index=30'
        get_page = requests.get(search_url, headers=other_type_name_headers)
        page_dic = get_page.json()
        if page_dic['error'] == 1:
            print('there is no content about %s' % keyword)
            return result_lst
        info_for_soup = page_dic['data']['feed_html']
        soup = BeautifulSoup(info_for_soup, 'html.parser')
        info_lst = soup.find_all('li', {'class': 'y-feed-item'})
        print('get page at page %s' % page_num)
        page_num += 1
        for line in info_lst:
            title = line.find('p', {'class': 'y-feed-title'}).text
            url = line.a['href']
            releaser_time_str = line.find('span', {'class': 'y-feed-desc-time'}).text
            release_time = trans_strtime_to_timestamp(releaser_time_str)
            source = line.find('span', {'class': 'y-feed-desc-source'}).text
            try:
                get_whole_page = requests.get(url)
                whole_page_text = get_whole_page.text
                print('get news %s' % title)
            except:
                print('get whole page process error %s' % title)
                whole_page_text = 'missing'
            info_dic = {'title': title,
                        'url': url,
                        'release_time': release_time,
                        'source': source,
                        'whole_page_text': whole_page_text}
            result_lst.append(info_dic)
    return result_lst


if __name__ == '__main__':
    keyword = '独行侠'
    max_page_num = 10
    result2 = shenma_info_page_other_type_name(keyword, max_page_num, kw_type='NBA')
