# -*- coding: utf-8 -*-
"""
Created on Wed Sep 26 10:51:05 2018

@author: fangyucheng
"""

import requests

proxy_dic = {'http': 'http://121.69.70.182:8118'}

url = 'http://192.168.17.13'

get_page = requests.get(url)
get_page.text

get_page2 = requests.get(url, proxies=proxy_dic, timeout=4)