# -*- coding: utf-8 -*-
"""
Created on Wed Sep 12 21:33:59 2018

@author: zhouyujiang

@edited by: fangyucheng
"""



def make_up_replace_sql(table_name, input_dic):
    replace_sql_first = 'REPLACE INTO {table_name}('.format(table_name=table_name)
    replace_sql_mind = ''
    replace_sql_end = ''
    for one_source in input_dic:
        replace_sql_mind = replace_sql_mind + '{one_source},'.format(one_source=one_source)
        replace_sql_end = replace_sql_end + '"{data}",'.format(data=input_dic[one_source])
    replace_sql_mind = replace_sql_mind[0:-1]
    replace_sql_mind = replace_sql_mind+') VALUES('
    replace_sql_end = replace_sql_end[0:-1]
    replace_sql_end = replace_sql_end+')'
    replace_sql = replace_sql_first + replace_sql_mind + replace_sql_end
    return replace_sql
