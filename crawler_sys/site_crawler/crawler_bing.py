# -*- coding: utf-8 -*-
"""
Created on Wed Aug 22 09:30:20 2018

@author: fangyucheng
"""


import time
import requests
from bs4 import BeautifulSoup
from crawler_sys.utils.trans_strtime_to_timestamp import trans_strtime_to_timestamp


def bing_page(keyword, max_page_num):
    result_lst = []
    for page_num in range(0, max_page_num):
        search_url = ('https://cn.bing.com/search?q=' + keyword + '&pc=MOZI&first=' 
                      + str(max_page_num*10) + '&FORM=PERE1')
        get_page = requests.get(search_url)
        get_page.encoding = 'utf-8'
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        info_lst = soup.find_all('li', {'class': 'b_algo'})
        print_page_num = page_num+1
        for line in info_lst:
            title = line.h2.a.text
            title = title.replace('\n', '')
            url = line.h2.a['href']
            release_time_and_content = line.find('p').text
            release_time_and_content_lst = release_time_and_content.split('\u2002·\u2002')
            content = release_time_and_content_lst[-1]
            release_time_str = release_time_and_content_lst[0]
            release_time = trans_strtime_to_timestamp(release_time_str)
            get_whole_page_str = line.find('div', {'class': 'b_attribution'})['u']
            get_whole_page_lst = get_whole_page_str.split('|')
            d_number = get_whole_page_lst[2]
            w_number = get_whole_page_lst[3]
            get_whole_page_url = ('http://cncc.bingj.com/cache.aspx?q=' + keyword +
                                  '&d=' + d_number + '&mkt=zh-CN&setlang=zh-CN&w='
                                  + w_number)
            get_whole_page = requests.get(get_whole_page_url)
            whole_page_html = get_whole_page.text
            fetch_time = int(time.time()*1000)
            info_dic = {'title': title,
                        'url': url,
                        'content': content,
                        'release_time': release_time,
                        'keyword': keyword,
                        'whole_page_html': whole_page_html,
                        'fetch_time': fetch_time}
            result_lst.append(info_dic)
            print('get data at page %s' % print_page_num)
    return result_lst


if __name__ == '__main__':
    keyword = '中超'
    test_data = bing_page(keyword, max_page_num=10)