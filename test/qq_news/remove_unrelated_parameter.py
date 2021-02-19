# -*- coding: utf-8 -*-
"""
Created on Mon Nov 26 13:14:03 2018

@author: fangyucheng
"""

import time
import urllib
import requests

domain = 'http://r.inews.qq.com/searchMore?'
#domain_simple = 'http://r.inews.qq.com/searchMore'
headers = {"Host": "r.inews.qq.com",
           "Accept-Encoding": "gzip,deflate",
           "Referer": "http://inews.qq.com/inews/android/",
           "User-Agent": "%E8%85%BE%E8%AE%AF%E6%96%B0%E9%97%BB5410(android)",
           "Cookie": "lskey=;luin=;skey=;uin=; logintype=0; main_login=qq;",
           "Connection": "Keep-Alive"}

url_dic = {'isoem':'0',
           'mid': '74b9305504a047ab0a1901e2dfbf71f87f799819',
           'dpi': '270',
           'devid': '008796749793280',
           'is_chinamobile_oem': '0',
           'mac': 'mac%20unknown',
           'real_device_width':'5.06',
           'store': '17',
           'screen_height': '1440',
           'real_device_height': '9.0',
           'apptype': 'android',
           'origin_imei': '008796749793280',
           'orig_store': '17',
           'hw': 'etease_MuMu',
           'appver': '23_android_5.4.10',
           'uid': '54767d8bf41ac9a4',
           'screen_width':'810',
           'sceneid':'',
           'omgid': '818b2ebf4abcec4bc1c8bf737a1c131dede60010213210',
           'timeline':'1540802766',
           'query':'espn',
           'activefrom': 'icon',
           'qqnetwork': 'wifi',
           'rom_type': '',
           'secId': '2',
           'Cookie':'lskey=;luin=;skey=;uin=; logintype=0; main_login=qq;',
           'network_type': 'wifi',
           'id': '20181029A18Y3H00',
           'global_info': '1|0|0|0|1|1|1|1|0|6|1|1|1|1|0|J060P000000000:B054P000011803|1402|0|0|-1|-1|0|0|0||-1|-1|0|0|1|1|0|0|-1|0|2|0|2|0|0|0|0|0|0|0|0|2|0|0|0|0',
           'imsi_history':'0,460013199570862',
           'omgbizid': 'a520b26ce7880445ab488481e3dd4949c74f0050213210',
           'qn-rid': '40ff49fc-e6ee-4384-8a7c-9ee507d57e47',
           'qn-sig': 'c022edce8ae72f053304412f13a9bb88',
           'page': "2",
           'type': "0",
           'imsi': '460013199570862'}

#sig = 'c022edce8ae72f053304412f13a9bb88'

url = 'http://r.inews.qq.com/searchMore?%s' % urllib.parse.urlencode(url_dic)

"""
raw-url
'http://r.inews.qq.com/searchMore?isoem=0&mid=74b9305504a047ab0a1901e2dfbf71f87f799819&dpi=270&devid=008796749793280&is_chinamobile_oem=0&mac=mac%2520unknown&real_device_width=5.06&store=17&screen_height=1440&real_device_height=9.0&apptype=android&origin_imei=008796749793280&orig_store=17&hw=etease_MuMu&appver=23_android_5.4.10&uid=54767d8bf41ac9a4&screen_width=810&sceneid=&omgid=818b2ebf4abcec4bc1c8bf737a1c131dede60010213210&timeline=1540802766&query=espn&activefrom=icon&qqnetwork=wifi&rom_type=&secId=2&Cookie=lskey%3D%3Bluin%3D%3Bskey%3D%3Buin%3D%3B+logintype%3D0%3B+main_login%3Dqq%3B&network_type=wifi&id=20181029A18Y3H00&global_info=1%7C0%7C0%7C0%7C1%7C1%7C1%7C1%7C0%7C6%7C1%7C1%7C1%7C1%7C0%7CJ060P000000000%3AB054P000011803%7C1402%7C0%7C0%7C-1%7C-1%7C0%7C0%7C0%7C%7C-1%7C-1%7C0%7C0%7C1%7C1%7C0%7C0%7C-1%7C0%7C2%7C0%7C2%7C0%7C0%7C0%7C0%7C0%7C0%7C0%7C0%7C2%7C0%7C0%7C0%7C0&imsi_history=0%2C460013199570862&omgbizid=a520b26ce7880445ab488481e3dd4949c74f0050213210&qn-rid=40ff49fc-e6ee-4384-8a7c-9ee507d57e47&qn-sig=c022edce8ae72f053304412f13a9bb88&page=2&type=0&imsi=460013199570862'
"""

get_page = requests.get(url, headers=headers)
page = get_page.text

key_lst = []
para_lst = []

for key, value in url_dic.items():
    key_lst.append(key)

for key in key_lst:
    value = url_dic[key]
    url_dic.pop(key)
    url = 'http://r.inews.qq.com/searchMore?%s' % urllib.parse.urlencode(url_dic)
    get_page = requests.get(url, headers=headers)
    page = get_page.text
    time.sleep(10)
    if len(page) > 5000:
        print("%s can be moved from url, length of page is %s" % (key, len(page)))
        continue
    else:
        url_dic[key] = value
        print("key %s, value %s can't be moved from url" % (key, value))

#this the result
final_url_dic = {'devid': '008796749793280',
                 'appver': '23_android_5.4.10',
                 'query': 'espn',
                 'qn-rid': '3e3cb605-3a00-412d-8b2e-f81b32f5064c',
                 'qn-sig': '589a406e354aa3bfb6ddeaa778278ef1'}
final_url = 'http://r.inews.qq.com/searchMore?%s' % urllib.parse.urlencode(final_url_dic)

"""
final_url
'http://r.inews.qq.com/searchMore?devid=008796749793280&appver=23_android_5.4.10&query=espn&qn-rid=40ff49fc-e6ee-4384-8a7c-9ee507d57e47&qn-sig=c022edce8ae72f053304412f13a9bb88'
"""

qn_sig = "01552dc75351b12d01b5310441a562e2"
qn_rid = "8f14a809-db96-4762-905a-7c063dc84ac7"
