# -*- coding: utf-8 -*-
"""
Created on Mon Sep 10 15:04:04 2018

@author: fangyucheng
"""

def trans_duration(duration_str):
    """suitable for 20:20, 20:20:10"""
    if type(duration_str) == int:
        return duration_str
    duration_lst = duration_str.split(':')
    if len(duration_lst) == 3:
        duration = int(int(duration_lst[0]) * 3600 + int(duration_lst[1]) * 60 + int(duration_lst[2]))
        return duration
    elif len(duration_lst) == 2:
        duration = int(int(duration_lst[0]) * 60 + int(duration_lst[1]))
        return duration
    else:
        return duration_lst[0]
