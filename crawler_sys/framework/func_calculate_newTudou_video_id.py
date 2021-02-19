# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 16:40:20 2017

@author: hanye
"""
import re
def calculate_newTudou_video_id(newTudou_url):
    try:
        d_url_s_Lst = newTudou_url.split('.html')
        d_videoID = d_url_s_Lst[0]
        newTudou_video_id = re.findall(r"/\w/(.+)?", d_videoID)[0]
    except:
        newTudou_video_id = None
    return newTudou_video_id

