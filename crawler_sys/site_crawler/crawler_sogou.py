# -*- coding: utf-8 -*-
"""
Created on Wed Aug 22 09:30:20 2018

@author: fangyucheng
"""


import time
import requests
from bs4 import BeautifulSoup
from crawler.crawler_sys.utils.trans_strtime_to_timestamp import trans_strtime_to_timestamp


cookie = ('YYID=2FFBDAA6D4FBA37438F4067C8123E98B; IMEVER=8.5.0.1322;'
          'SUID=3D03FF723865860A59795A5F000BB71F;'
          'SUV=00C039A172FF033D5993ADBD770E7410; usid=lF0F7il0yWbXF5c9;'
          'IPLOC=CN1100; sct=11; SMYUV=1512954490386200;'
          'ad=19fxxkllll2zKxvnlllllVHr6$UllllltsDRlyllll9llllljgDll5@@@@@@@@@@;'
          'SNUID=D0DE5A671A1E68C31FB628911B8277A5; wuid=AAGPcSphIAAAAAqLE2OSTQgAGwY=;'
          'UM_distinctid=16449b02797449-0c5d9293f4a833-143f7040-1fa400-16449b02799881;'
          'CXID=794EC592A14CE76F5DF3F3A3BDDDD787;'
          'ld=Kyllllllll2bWX10QTIdJOHDsvSbWX1uK94Vhkllll9lllllVklll5@@@@@@@@@@;'
          'cd=1534754086&17502a3f56c02f72dfd43a17cbb19663;'
          'rd=Vyllllllll2bBEqoQLWCNCHfKv2bWX1uzX0atkllllwllllRVllll5@@@@@@@@@@;'
          'LSTMV=173%2C72; LCLKINT=1570')


headers = {'Host': 'news.sogou.com',
           'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:61.0) Gecko/20100101 Firefox/61.0',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
           'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
           'Accept-Encoding': 'gzip, deflate',
           'Cookie': cookie,
           'Connection': 'keep-alive',
           'Upgrade-Insecure-Requests': '1',
           'Cache-Control': 'max-age=0'}


def sogou_info_page(keyword):
    result_lst = []
    for page_num in range(1,11):
        search_url = 'http://news.sogou.com/news?&query='+keyword+'&page='+str(page_num)
        get_page = requests.get(search_url, headers=headers)
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        news_lst = soup.find_all('div', {'class': 'vrwrap'})
        for line in news_lst:
            try:
                title = line.div.h3.a.text
                url = line.div.h3.a['href']
                source_and_release_time = line.find('p', {'class': 'news-from'}).text
                source_and_release_time_lst = source_and_release_time.split('\xa0')
                source = source_and_release_time_lst[0]
                release_time_str = source_and_release_time_lst[-1]
                release_time = trans_strtime_to_timestamp(release_time_str)
                try:
                    content = line.find('span').text
                except:
                    print('no content at %s' % title)
                    content = 'missing'
                fetch_time = int(time.time()*1000)
                try:
                    similar_news = line.find('a', {'id': 'news_similar'}).text
                except:
                    print('no similar news at %s' % title)
                    similar_news = 'missing'
                news_info = {'title': title,
                             'url': url,
                             'source': source,
                             'release_time': release_time,
                             'fetch_time': fetch_time,
                             'content': content,
                             'similar_news': similar_news,
                             'keyword': keyword}
                result_lst.append(news_info)
                print('get data at page %s' % page_num)
            except:
                ('the error occured at position %s' % news_lst.index(line))
    return result_lst


if __name__=='__main__':
    keyword = '中超'
    test_sogou = sogou_info_page(keyword)
    
