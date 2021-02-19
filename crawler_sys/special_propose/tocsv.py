# -*- coding: utf-8 -*-
"""
Created on Wed Mar 28 01:12:09 2018

@author: Administrator
"""


import pandas as pd
ttt=pd.DataFrame(comment)
ttt['heiheihei']="'"
ttt['id']=ttt['heiheihei']+ttt['contentid']
ttt['real_time']=pd.to_datetime(ttt['publishdate'],unit='s')
try:
    ttt.to_csv('wuwuwu.csv',encoding='utf-8',index=False)
except UnicodeEncodeError:
    pass
