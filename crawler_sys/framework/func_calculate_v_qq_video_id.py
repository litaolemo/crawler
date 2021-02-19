# -*- coding: utf-8 -*-
"""
Created on Mon Nov  6 09:54:09 2017

@author: hanye
"""
import re

def calculate_v_qq_video_id(v_qq_page_url):
    find_vid = re.findall('/[0-9a-zA-Z]+.html', v_qq_page_url)
    if find_vid != []:
        vid = find_vid[0].split('/')[1].split('.')[0]
    else:
        vid = v_qq_page_url
    return vid
