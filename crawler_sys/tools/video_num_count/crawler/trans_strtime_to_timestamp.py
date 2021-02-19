# -*- coding: utf-8 -*-
"""
Created on Wed Aug 22 10:57:16 2018

将形如‘xx分钟前’‘xx小时前’的时间信息转化为时间戳

@author: fangyucheng
"""

import re
import time
import datetime

def trans_strtime_to_timestamp(input_time, missing_year=False):
    now = datetime.datetime.now()
    real_time = None
    input_time = input_time.replace('\n', '')
    input_time = input_time.replace('\t', '')
    def func_inyear(input_time):
        try:
            if '-' in input_time:
                in_month = int(input_time.split('-')[0])
                now = datetime.datetime.now()
                now_month = now.month
                now_year = now.year
                if in_month > now_month:
                    in_year = now_year -1
                elif in_month <= now_month:
                    in_year = now_year
                else:
                    in_year = 0
                return str(in_year)
        except:
            return 0
    if '昨天' in input_time:
        if len(input_time)>2:
            try:
                real_time_lst = re.findall('\d+', input_time)
                mint = int(real_time_lst[0])
                ss = int(real_time_lst[1])
                now_day = datetime.datetime.now()
                real_time_dt = datetime.datetime(now_day.year, now_day.month,
                                                 now_day.day-1, mint, ss)
                real_time = int(datetime.datetime.timestamp(real_time_dt)*1000)
            except:
                real_time = int((time.time()-24*3600)*1e3)
        else:
            real_time = int((time.time()-24*3600)*1e3)
        return real_time
    if '前天' in input_time:
        if len(input_time)>2:
            try:
                real_time_lst = re.findall('\d+', input_time)
                mint = int(real_time_lst[0])
                ss = int(real_time_lst[1])
                now_day = datetime.datetime.now()
                real_time_dt = datetime.datetime(now_day.year, now_day.month,
                                                 now_day.day-2, mint, ss)
                real_time = int(datetime.datetime.timestamp(real_time_dt)*1000)
            except:
                real_time = int((time.time()-2*24*3600)*1e3)
        else:
            real_time = int((time.time()-2*24*3600)*1e3)
        return real_time
    if '分钟' in input_time:
        real_time_lst = re.findall('\d+', input_time)
        real_time_int = int(' '.join(real_time_lst))
        real_time = int((time.time()-real_time_int*60)*1e3)
        return real_time
    if '小时' in input_time:
        real_time_lst = re.findall('\d+', input_time)
        real_time_int = int(' '.join(real_time_lst))
        real_time = int((time.time()-real_time_int*3600)*1e3)
        return real_time
    if '天' in input_time:
        real_time_lst = re.findall('\d+', input_time)
        real_time_int = int(' '.join(real_time_lst))
        real_time = int((time.time()-real_time_int*24*3600)*1e3)
        return real_time
    if ('月' in input_time or '月前' in input_time) and "日" not in input_time:
        real_time_lst = re.findall('\d+', input_time)
        real_time_int = int(' '.join(real_time_lst))
        real_time = int((time.time()-real_time_int*30*24*3600)*1e3)
        return real_time
    if '月' in input_time and '日' in input_time and '年' not in input_time:
        input_time_replace_chinese = str(now.year) + "-" + input_time.replace('月', '-').replace('日', '')
        if len(input_time_replace_chinese) == 16:
            real_time = int(datetime.datetime.strptime(input_time_replace_chinese,
                                                          '%Y-%m-%d %H:%M').timestamp()*1e3)
            return real_time
        elif len(input_time_replace_chinese) == 20:
            real_time = int(datetime.datetime.strptime(input_time_replace_chinese,
                                                       '%Y-%m-%d %H:%M:%S').timestamp()*1e3)
            return real_time
        elif len(input_time_replace_chinese) == 8 or len(input_time_replace_chinese) == 9 or len(input_time_replace_chinese) == 10:
            real_time = int(datetime.datetime.strptime(input_time_replace_chinese,
                                                       '%Y-%m-%d').timestamp()*1e3)
            return real_time
    if '年' in input_time and '月' in input_time and '日' in input_time:
        input_time_replace_chinese = input_time.replace('年', '-').replace('月', '-').replace('日', '')
        if len(input_time_replace_chinese) == 16:
            real_time = int(datetime.datetime.strptime(input_time_replace_chinese, 
                                                          '%Y-%m-%d %H:%M').timestamp()*1e3)
            return real_time
        elif len(input_time_replace_chinese) == 20:
            real_time = int(datetime.datetime.strptime(input_time_replace_chinese, 
                                                       '%Y-%m-%d %H:%M:%S').timestamp()*1e3)
            return real_time
        elif len(input_time_replace_chinese) == 8 or len(input_time_replace_chinese) == 9 or len(input_time_replace_chinese) == 10:
            real_time = int(datetime.datetime.strptime(input_time_replace_chinese, 
                                                       '%Y-%m-%d').timestamp()*1e3)
            return real_time
    length = len(input_time)
    if real_time is None and length in [8, 9, 10]:
        real_time = int(datetime.datetime.strptime(input_time, '%Y-%m-%d').timestamp()*1e3)
        return real_time
    if real_time is not None:
        return ''
    if missing_year == True and len(input_time) > 5:
        year = func_inyear(input_time)
        if year != str(0):
            input_time = year + '-' + input_time
            real_time = int(datetime.datetime.strptime(input_time, 
                                                               '%Y-%m-%d').timestamp()*1e3)
        else:
            print('error in {input_time}'.format(input_time=input_time))
        return real_time
    #for str_time %Y-%m
    if len(input_time) == 5 :
        year = func_inyear(input_time)
        if year != str(0):
            input_time = year + '-' + input_time
            real_time = real_time = int(datetime.datetime.strptime(input_time, 
                                                               '%Y-%m-%d').timestamp()*1e3)
        else:
            print('error in {input_time}'.format(input_time=input_time))
        return real_time
    if real_time is None:
        real_time = 0
        print('unsuitable format %s' % input_time)
        return real_time
 
    