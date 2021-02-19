# -*- coding: utf-8 -*-
"""
Created on Tue Jul  3 09:56:46 2018

@author: hanye
"""

from crawler_sys.framework.func_calculate_newTudou_video_id import calculate_newTudou_video_id
from crawler_sys.framework.func_calculate_toutiao_video_id import calculate_toutiao_video_id
from crawler_sys.framework.func_calculate_v_qq_video_id import calculate_v_qq_video_id
from crawler_sys.framework.func_calculate_kwai_video_id_by_url import calculate_kwai_video_id_by_data_by_url as calculate_kwai_video_id
from crawler_sys.framework.func_calculate_txxw_video_id import calculate_txxw_video_id
from crawler_sys.framework.func_calculate_wangyi_news_id import calculate_wangyi_news_id
from crawler_sys.framework.func_calculate_douyin_id import calculate_douyin_id
from crawler_sys.framework.func_get_releaser_id import get_releaser_id


def vid_cal_func(platform):
    vid_cal_func_dict = {
        'toutiao': calculate_toutiao_video_id,
        'new_tudou': calculate_newTudou_video_id,
        '腾讯视频': calculate_v_qq_video_id,
        'kwai': calculate_kwai_video_id,
        '腾讯新闻':calculate_txxw_video_id,
        "网易新闻":calculate_wangyi_news_id,
        "抖音":calculate_douyin_id
        }

    def general_vid_cal_func(url):
        return url

    if platform in vid_cal_func_dict:
        return vid_cal_func_dict[platform]
    else:
        return general_vid_cal_func
