# -*- coding: utf-8 -*-
"""
Created on Wed Oct 10 17:06:49 2018

@author: fangyucheng
"""

import os
import configparser

conf_file_path = os.getcwd()
#initialize conf file
config = configparser.ConfigParser()
config['腾讯新闻'] = {'keyword': '看看新闻,看看新闻Knews,Knews'}
with open('D:/python_code/crawler/crawler_sys/framework/config/search_keywords.ini',
          'w', encoding='utf-8') as configfile:
    config.write(configfile)
print(os.getcwd())

config = configparser.ConfigParser()
config.read('D:/python_code/crawler/crawler_sys/framework/config/search_keywords.ini')