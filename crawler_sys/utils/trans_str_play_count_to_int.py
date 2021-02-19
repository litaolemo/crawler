# -*- coding: utf-8 -*-
"""
Created on Mon Sep 10 14:41:54 2018

@author: fangyucheng
"""

def trans_play_count(play_count_str):
    """suitable for the format 22万, 22万次播放, 22.2万, 2,222万, 2,222.2万, 2,222, 222"""
    if isinstance(play_count_str,int):
        return play_count_str

    play_count_str = play_count_str.replace('次播放', '')
    play_count_str = play_count_str.replace('播放', '')
    try:
        if '万' in play_count_str:
            play_count_str = play_count_str.split('万')[0]
            if ',' in play_count_str:
                play_count_str = play_count_str.replace(',', '')
            play_count = int(float(play_count_str) * 1e4)
            return play_count
        elif "w" in play_count_str:
            play_count_str = play_count_str.split('w')[0]
            if ',' in play_count_str:
                play_count_str = play_count_str.replace(',', '')
            play_count = int(float(play_count_str) * 1e4)
            return play_count
        else:
            try:
                play_count = int(play_count_str)
            except:
                play_count = int(play_count_str.replace(',', ''))
            return play_count
    except:
        return None