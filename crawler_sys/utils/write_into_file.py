# -*- coding: utf-8 -*-
"""
Created on Thu Oct 11 09:04:54 2018

@author: fangyucheng
"""


import datetime
import json

current_date = datetime.datetime.now().isoformat()[:10]

def write_str_into_file(file_path,
                        file_name,
                        var):
    with open(file_path+file_name+current_date, 'a', encoding='utf-8') as file:
        file.write(var)
        file.write('\n')

def write_dic_into_file(file_path,
                        file_name,
                        var):
    with open(file_path+file_name+current_date, 'a', encoding='utf-8') as file:
        var_json = json.dumps(var)
        file.write(var_json)
        file.write('\n')
