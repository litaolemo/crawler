# -*- coding: utf-8 -*-
"""
Created on Mon May 14 13:35:26 2018

@author: fangyucheng
"""


import json
import pandas as pd


def dic_file_to_lst(filename):
    openfile = open(filename)
    task = []
    for line in openfile:
        line_dic = json.loads(line)
        task.append(line_dic)
    return task


def str_file_to_lst(filename):
    openfile = open(filename, 'r', encoding='utf-8')
    task = []
    for line in openfile:
        line_str = line.replace('\n','') 
        task.append(line_str)
    return task


def csv_to_lst(csvname):
    openfile = open(csvname, 'r')
    task = []
    for line in openfile:
        task.append(line)
    task_lst = []
    for line in task:
        line_lst = line.split(',')
    task_lst.append(line_lst)
    return task_lst


def csv_to_lst_with_headline(csvname):
    result_lst = []
    openfile = open(csvname, 'r', encoding='gb18030')
    head = openfile.readline()
    head = head.replace('\n', '')
    head_lst = head.strip().split(',')
    for line in openfile:
        line = line.replace('\n', '')
        line_lst = line.strip().split(',')
        test_dict = dict(zip(head_lst,line_lst))
        result_lst.append(test_dict)
    return result_lst


def dic_lst_to_file(listname, filename):
    file = open(filename, 'a')
    for line in listname:
        json_line = json.dumps(line)
        file.write(json_line)
        file.write('\n')
    file.flush()
    file.close()


def str_lst_to_file(listname, filename):
    file = open(filename, 'a', encoding='gb18030')
    for line in listname:
        file.write(line)
        file.write('\n')
    file.flush()
    file.close()


def lst_to_csv_practical(lst_name, csv_name):
    dataframe=pd.DataFrame(lst_name)
    if len(str(lst_name[0]['release_time'])) == 10:
        dataframe['midstep'] = dataframe['release_time']+8*3600
        dataframe['realtime'] = pd.to_datetime(dataframe['midstep'],unit='s')
    else:
        dataframe['midstep'] = dataframe['release_time']+8*3600*1e3
        dataframe['realtime'] = pd.to_datetime(dataframe['midstep'],unit='ms')
    del dataframe['midstep']
    del dataframe['release_time']
    del dataframe['fetch_time']
    dataframe.to_csv(csv_name, encoding='gb18030', index=False)


def lst_to_csv(listname, csvname):
    dataframe = pd.DataFrame(listname)
    dataframe.to_csv(csvname, encoding='gb18030', index=False)


def csv_to_file(filename, csvname):
    file = open(filename)
    task = []
    for line in task:
        json_line = json.dumps(line)
        file.write(json_line)
        file.write('\n')
    file.flush()
    dataframe = pd.DataFrame(task)
    dataframe.to_csv(csvname, encoding='gb18030', index=False)
    file.close()
