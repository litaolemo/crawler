# -*- coding: utf-8 -*-
"""
Created on Tue Sep 25 16:23:15 2018

@author: fangyucheng
"""



def build_proxy_dic(raw_proxy_dic):
    """
    build proxy_dic by dic from mysql database
    """
    cate = raw_proxy_dic['category']
    port = raw_proxy_dic['port']
    ip_address = raw_proxy_dic['ip_address']
    proxy_body = cate + '://' + ip_address + ':' + port
    proxy_dic = {cate: proxy_body}
    return proxy_dic