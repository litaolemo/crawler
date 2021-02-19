# -*- coding: utf-8 -*-
"""
Created on Tue Aug 21 16:58:37 2018

@author: fangyucheng
"""

import time
import requests
from bs4 import BeautifulSoup
from crawler.crawler_sys.utils.trans_strtime_to_timestamp import trans_strtime_to_timestamp


headers = {'Host': 'www.baidu.com',
           'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:61.0) Gecko/20100101 Firefox/61.0',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
           'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
           'Accept-Encoding': 'gzip, deflate, br',
           'Cookie': 'BAIDUID=5EBFCC8E193341115A4A3C71960B63E7:FG=1; BIDUPSID=BD339F6B0442001D2528C4BFBCE098DB; PSTM=1500974423; BDUSS=RCY0lFRmJ4MDlMMU5xfkp4NWU3bUlTckJOZU03ZTB4UHdJbUpUeWlVZmhlT3haSVFBQUFBJCQAAAAAAAAAAAEAAABM1D8MZmFuZzExMDExNAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAOHrxFnh68RZc3; MCITY=-131%3A; BD_UPN=1352; H_PS_PSSID=; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; delPer=1; BD_CK_SAM=1; PSINO=2; BDRCVFR[gltLrB7qNCt]=mk3SLVN4HKm; pgv_pvi=525595648; pgv_si=s2288931840; Hm_lvt_9f14aaa038bbba8b12ec2a4a3e51d254=1534841172; Hm_lpvt_9f14aaa038bbba8b12ec2a4a3e51d254=1534841172; BD_HOME=1; sug=3; sugstore=0; ORIGIN=0; bdime=21110; BDRCVFR[feWj1Vr5u3D]=I67x6TjHwwYf0; BDSVRTM=225; BDRCVFR[C0p6oIjvx-c]=I67x6TjHwwYf0; BAIDUPH=tn=Â§rn=Â§ct=0',
           'Connection': 'keep-alive',
           'Upgrade-Insecure-Requests': '1',
           'Cache-Control': 'max-age=0'}


def baidu_info_page(keyword, max_page_num):
    result_lst = []
    for page_num in range(0, max_page_num):
        search_url = ('https://www.baidu.com/s?tn=news&rtt=4&bsst=1&cl=2&wd='+keyword+
                      '&x_bfe_rqs=03E80&tngroupname=organic_news&pn='+str(page_num*10))
        get_page = requests.get(search_url, headers=headers)
        get_page.encoding = 'utf-8'
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        info_lst = soup.find_all('div', {'class': 'result'})
        print_page_num = page_num+1
        for line in info_lst:
            title = line.h3.a.text
            title = title.replace('\n', '')
            url = line.h3.a['href']
            source_and_release_time = line.find('p', {'class': 'c-author'}).text
            source_and_release_time_lst = source_and_release_time.split('\xa0')
            source = source_and_release_time_lst[0]
            release_time_str = source_and_release_time_lst[-1]
            release_time = trans_strtime_to_timestamp(release_time_str)
            midstep_content = line.find('div', {'class': 'c-summary'}).text
            content = midstep_content.replace(source, '').replace(' ', '')
            content = content.replace('\xa0', '')
            source = source.replace('\n', '').replace('\t', '')
            content = content.replace('\n', '').replace('\t', '')
            whole_page = line.find('a', {'class': 'c-cache'})['href']
            fast_open_whole_page = whole_page + '&fast=y'
            get_whole_page = requests.get(fast_open_whole_page, headers=headers)
            get_whole_page.encoding = 'gb18030'
            whole_page_html = get_whole_page.text
            fetch_time = int(time.time()*1000)
            info_dic = {'title': title,
                        'url': url,
                        'source': source,
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
    test_data = baidu_info_page(keyword, max_page_num=10)