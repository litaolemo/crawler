# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 10:03:53 2018

@author: fangyucheng
"""

import configparser
from crawler.crawler_sys.utils.trans_format import csv_to_lst_with_headline

task_list = csv_to_lst_with_headline('F:/add_target_releaser/last_month/zhangminghui2.csv')

releaser_dic = {}
for line in task_list:
    releaser_dic[line['releaser']] = line['releaserUrl']


config = configparser.ConfigParser()
config['haokan'] = releaser_dic

with open ('key_customer.ini', 'w', encoding='utf-8') as ini:
    config.write(ini)



#special task
#for line in source_lst:
#    detail_lst = line['detail']
#    csm_mdu = detail_lst[0]['csm_mdu']
#    for detail_dic in detail_lst:
#        detail_dic.pop('csm_mdu')
#    line['csm_mdu'] = csm_mdu