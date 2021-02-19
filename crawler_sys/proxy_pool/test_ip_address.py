# -*- coding: utf-8 -*-
"""
Created on Tue Sep 18 13:40:51 2018

@author: fangyucheng
"""


import requests
from crawler_sys.proxy_pool.connect_with_database import update_status
from crawler_sys.proxy_pool.connect_with_database import extract_proxy_to_test


def test_ip():
    test_lst = extract_proxy_to_test(host='localhost',
                                     passwd='goalkeeper@1')
    result_lst = []
    for line in test_lst:
        cate = line['category']
        cate = cate.lower()
        ip_address = line['ip_address']
        port = line['port']
        proxy = cate + '://' + ip_address + ':' + port
        proxy_dic = {cate: proxy}
        record_id = line['id']
        try:
            get_page = requests.get('http://service.cstnet.cn/ip', proxies=proxy_dic, timeout=8)
            response_time = get_page.elapsed
            update_status(record_id=record_id,
                          host='localhost',
                          passwd='goalkeeper@1')
            print('%s is useful with response time %s' % (ip_address, response_time))
        except:
            update_status(record_id=record_id, 
                          availability=0,
                          host='localhost',
                          passwd='goalkeeper@1')
            print('%s is not useful' % ip_address)
    return result_lst


if __name__ == '__main__':
    test_ip()