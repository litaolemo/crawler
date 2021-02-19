# -*- coding: utf-8 -*-
"""
Created on Tue Sep 18 19:45:01 2018

@author: fangyucheng
"""


import requests
from requests.packages.urllib3.connectionpool import HTTPConnectionPool

def _make_request(self, conn, method, url, **kwargs):
    response = self._old_make_request(conn, method, url, **kwargs)
    sock = getattr(conn, 'sock', False)
    if sock:
        setattr(response, 'peer', sock.getpeername())
    else:
        setattr(response, 'peer', None)
    return response

HTTPConnectionPool._old_make_request = HTTPConnectionPool._make_request
HTTPConnectionPool._make_request = _make_request


proxy_dic = {'http': 'http://183.154.215.78:9000'}
r = requests.get('http://www.baidu.com', proxies=proxy_dic, timeout=8)
print (r.raw._original_response.peer)