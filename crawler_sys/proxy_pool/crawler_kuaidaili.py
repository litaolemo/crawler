# -*- coding: utf-8 -*-
"""
Created on Tue Sep 18 10:32:21 2018

@author: fangyucheng
"""


import time
import requests
import datetime
from bs4 import BeautifulSoup
from crawler_sys.utils.write_into_database import write_lst_into_database

def kuaidaili(max_page=20):
    """
    get ip address from website kuaidaili
    """
    page_num = 1
    while page_num < max_page:
        create_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
        url = 'https://www.kuaidaili.com/free/inha/' + str(page_num) + '/'
        get_page = requests.get(url)
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        ip_lst = soup.find('tbody').find_all('tr')
        result_lst = []
        for line in ip_lst:
            ip_dic = {}
            ip_dic['ip_address'] = line.find('td', {'data-title': 'IP'}).text
            ip_dic['port'] = line.find('td', {'data-title': 'PORT'}).text
            ip_dic['category'] = line.find('td', {'data-title': '类型'}).text
            ip_dic['anonymity'] = line.find('td', {'data-title': '匿名度'}).text
            ip_dic['create_time'] = create_time
            ip_dic['availability'] = 1
            ip_dic['source'] = 'kuaidaili'
            result_lst.append(ip_dic)
        write_lst_into_database(data_lst=result_lst,
                                host='localhost',
                                user='root',
                                passwd='goalkeeper@1',
                                database_name='proxy_pool',
                                table_name='proxy_pool')
        print('finished page %s' % page_num)
        page_num += 1
        time.sleep(5)

if __name__ == '__main__':
    kuaidaili(max_page=30)