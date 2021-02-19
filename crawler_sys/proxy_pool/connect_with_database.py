# -*- coding: utf-8 -*-
"""
Created on Tue Sep 18 09:32:41 2018

@author: fangyucheng
"""

import datetime
import pymysql
from crawler_sys.proxy_pool import make_up_sql


def write_dic_into_database(data_dic,
                            host='192.168.18.11',
                            user='root',
                            passwd='csm@1234',
                            database_name='proxy_pool',
                            table_name='proxy_pool'):
    """
    write ip address dict in mysql database
    """
    connection = pymysql.connect(host=host, user=user, passwd=passwd,
                                 db=database_name, port=3306,
                                 cursorclass=pymysql.cursors.DictCursor)
    cursor = connection.cursor()

    write_into_sql = make_up_sql.make_up_replace_sql(table_name=table_name, input_dic=data_dic)
    cursor.execute(write_into_sql)
    connection.commit()
    print('write %s into database' % data_dic['ip_address'])


def write_lst_into_database(data_lst, table_name, database_name='proxy_pool'):
    """
    write ip address list in mysql database
    """
    connection = pymysql.connect(host='localhost', user='root', passwd='goalkeeper@1',
                                 db=database_name, port=3306,
                                 cursorclass=pymysql.cursors.DictCursor)
    cursor = connection.cursor()
    length = len(data_lst)
    for line in data_lst:
        write_into_sql = make_up_sql.make_up_replace_sql(table_name=table_name, input_dic=line)
        cursor.execute(write_into_sql)

    connection.commit()
    print('write %s pieces of data into database' % length)


def extract_data_to_test(host='192.168.18.11',
                         user='root',
                         passwd='csm@1234',
                         table_name='proxy_pool', 
                         database_name='proxy_pool'):
    """
    extract ip address from mysql database to test whether it is useful
    """
    connection = pymysql.connect(host=host, user=user, passwd=passwd,
                                 db=database_name, port=3306,
                                 cursorclass=pymysql.cursors.DictCursor)
    cursor = connection.cursor()
    search_sql = "select * from " + table_name + " where availability=1"
    cursor.execute(search_sql)
    data_lst = cursor.fetchall()
    return data_lst


def update_status(record_id,
                  availability=1,
                  host='192.168.18.11',
                  user='root',
                  passwd='csm@1234',
                  table_name='proxy_pool', 
                  database_name='proxy_pool'):
    """
    update status to figure out which record is tested
    """
    connection = pymysql.connect(host=host, user=user, passwd=passwd,
                                 db=database_name, port=3306,
                                 cursorclass=pymysql.cursors.DictCursor)
    cursor = connection.cursor()
    update_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
    update_sql = ("update " + table_name + " set availability=" + str(availability) + 
                  ", update_time='" + update_time + "' where id=" + str(record_id) )
    cursor.execute(update_sql)
    connection.commit()
    print('success commit record_id=%s' % record_id)

def extract_data_to_use(host='192.168.18.11',
                        user='root',
                        passwd='csm@1234',
                        table_name='proxy_pool', 
                        database_name='proxy_pool'):

    """
    extract ip address from mysql database to test whether it is useful
    """
    connection = pymysql.connect(host=host, user=user, passwd=passwd,
                                 db=database_name, port=3306,
                                 cursorclass=pymysql.cursors.DictCursor)
    cursor = connection.cursor()
    search_sql = "select * from " + table_name + " where availability=1 order by update_time desc"
    cursor.execute(search_sql)
    data_lst = cursor.fetchall()[0]
    return data_lst

def extract_proxy_to_test(host='192.168.18.11',
                          user='root',
                          passwd='csm@1234'):
    """
    extract ip address from mysql database to test whether it is useful
    """
    connection = pymysql.connect(host=host, user=user, passwd=passwd,
                                 db='proxy_pool', port=3306,
                                 cursorclass=pymysql.cursors.DictCursor)
    cursor = connection.cursor()
    search_sql = "select * from proxy_pool where availability=1"
    cursor.execute(search_sql)
    data_lst = cursor.fetchall()
    return data_lst