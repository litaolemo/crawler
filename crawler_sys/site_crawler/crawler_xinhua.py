# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 21:50:08 2018

@author: fangyucheng
"""


import requests

url = 'https://xhpfmapi.zhongguowangshi.com/v500/core/indexlist'

headers = {"Accept-Encoding": "*",
           "Content-Type": "application/json; charset=UTF-8",
           "Content-Length": "870",
           "Host": "xhpfmapi.zhongguowangshi.com",
           "Connection": "Keep-Alive",
           "User-Agent": "okhttp/3.10.0"}


post_dict = ('{"param":"vPCnptKLNQB9Bldt193BqMWKnVNcVmaQ4pcJ6k8iV5wmESJSidV85U3rbdOK'
             'e+jmLQUDdj8Ca9QuYHizMT6l7Vq3CKOBgtNieEbRQihk10GoWKkS+L9BWI9M '
             'd7Iq3IT/HL4saSsu6fkEe3PYjuA6EXKGPF6EThfdaQ3CyGPP+KhVFGp3C1Fk '
             '9U50pbC0HflUGTK1iH7U7A9ZKNjhNd07/U49uddfqZ8OFbZjBw4mwRij0tGP '
             'F5jXpIBKoNsSnpWNTQnL86VCLd7+9jmQ+PzCkdmEiYNqRPYzXv+ihAhvBNY9 '
             'yg/18dVE1+zwHR685iDwabVLWppSxacQJr7iz1uuc6O0hoIWiUBkhDcUsD8S '
             'O3/DXZ8PgLMEvagO4TfS4AqMy/n5rW5UwzYr+x8jKDpr0kNjrUUH2vvXlQr6 '
             '+TfqwS5qHm6+nGCLoyv7HyMoOmvSraDmg3OD66rGTol/Ri9NYlSrNpyYemGG '
             'fAuUuZ1pIzNeTzpF02TvzF2OQ8T2iROkmUK8iSXAuViE4I+KNKq959APFJ6Y '
             'Fs+i7nfZ0d2/5jonslonNYfHqAtotAuciwEW+fqy9aZpHMRQaU5XjUeVV0Sk '
             'dKiY2SveT59VTiOBgi8fy+q096BYfC+vPBp780AE0A8UnpgWz6LdyyP4GR0J '
             'JTVN6F4TiPnD5mA7Lr4fpUPlVtNt8BfGKS/AkIGD+BaLVxArmlbC/6A9/caI '
             'ldYWwg4yIu+CLjkDtif2NUwsqgfbgfbj/pbhPZGVzC+KjCP382OmQHKM4HoO '
             'tnJueIoPIb14EwBSz98qmJ6tMBJa2BxsSVbKV076QBE7qNiJF6ZFBimSwob8 '
             'upM="}')

get_page = requests.post(url, data=post_dict, headers=headers, verify=False)

page = get_page.json()