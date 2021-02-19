# -*- coding: utf-8 -*-
"""
Created on Mon Nov  6 09:54:09 2017

@author: hanye
"""
import re

def calculate_toutiao_video_id(toutiao_url):
    if toutiao_url[-1] != '/':
        toutiao_url = toutiao_url + '/'
    find_vid = re.findall('[0-9]+/', toutiao_url)
    if find_vid!=[]:
        vid = find_vid[0].replace('/', '')
        return vid
    else:
        return None


