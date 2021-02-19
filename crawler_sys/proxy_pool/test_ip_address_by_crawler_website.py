# -*- coding: utf-8 -*-
"""
Created on Tue Sep 18 23:14:06 2018

@author: fangyucheng
"""


import requests
from bs4 import BeautifulSoup
from crawler_sys.proxy_pool import connect_with_database

proxy_lst = connect_with_database.extract_data_to_use()

for line in proxy_lst:
    line['proxy_dic'] = {line['category']: line['whole_ip_address']}

result_lst = []
for line in proxy_lst:
    proxy_dic = line['proxy_dic']
    try:
        get_page = requests.get(url='http://service.cstnet.cn/ip', proxies=proxy_dic, timeout=8)
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        ip_address = soup.find('span', {'class': 'ip-num'}).text
        line['test_result'] = ip_address
        print('%s passed the test' % ip_address)
    except:
        print('failed passed the test')
        line['test_result'] = 'failed'
    result_lst.append(line)
    proxy_lst.remove(line)
