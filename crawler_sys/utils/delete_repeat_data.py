# -*- coding: utf-8 -*-
"""
Created on Sun Sep 30 09:36:55 2018

@author: fangyucheng
"""


import pymysql
from crawler_sys.utils.write_into_database import write_lst_into_database

connection = pymysql.connect(host='localhost', 
                             user='root',
                             passwd='goalkeeper@1',
                             db='proxy_pool', 
                             port=3306,
                             cursorclass=pymysql.cursors.DictCursor)
cursor = connection.cursor()

search_sql = "select * from proxy_pool"
cursor.execute(search_sql)

test_lst = cursor.fetchall()

new_lst = []
ip_lst = []
for line in test_lst:
    if line['ip_address'] not in ip_lst:
        new_lst.append(line)
        ip_lst.append(line['ip_address'])

delect_sql = "delete from proxy_pool where id >= 1"
cursor.execute(delect_sql)
connection.commit()

write_lst_into_database(data_lst=new_lst,
                        table_name='proxy_pool',
                        host='localhost',
                        passwd='goalkeeper@1')