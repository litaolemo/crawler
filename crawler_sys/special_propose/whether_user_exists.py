# -*- coding: utf-8 -*-
"""
Created on Fri Jun 15 09:49:15 2018

@author: fangyucheng
"""

from selenium import webdriver
from crawler_sys.utils.Metaorphosis import Meta
import time
import json

        
def open_url(filename,resultname):
    result_file = open(resultname,'a')
    result_lst = []
    browser = webdriver.Chrome()
    browser.maximize_window()
    time.sleep(60)
    meta = Meta()
    url_lst = meta.str_file_to_lst(filename)
    for url in url_lst:
        browser.get(url)
        time.sleep(6)
        try:
            user_name = browser.find_element_by_class_name('username').text
        except:
            user_name = None
        D0 = {'url':url,'user_name':user_name}
        json_D0 = json.dumps(D0)
        result_file.write(json_D0)
        result_file.write('\n')
        result_file.flush()
        result_lst.append(D0)
    return result_lst
        
    
if __name__=='__main__':
    filename = 'D:/CSM3.0/打杂/whether_exists.txt'
    resultname = 'D:/CSM3.0/打杂/whether_exists'
    result=open_url(filename,resultname)
