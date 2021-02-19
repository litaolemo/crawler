# -*- coding: utf-8 -*-
"""
Created on Mon Aug  6 15:53:10 2018

@author: zhouyujiang
"""

import re

def calculate_kwai_video_id_by_data_by_url(kwai_url):
    doc_id_str = re.findall(r"/u/(.+)?",kwai_url)
    if doc_id_str!=[]:
        vid = doc_id_str[0].replace('/','_')
        return vid
    else:
        return None
