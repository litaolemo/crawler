# -*- coding:utf-8 -*-
# @Time : 2019/6/11 15:26 
# @Author : litao
import datetime

from crawler.haokan_count import *
from crawler.qq_video_count import *
from crawler.toutiao_count import *
from crawler.tudou_count import *
from crawler.crawler_wangyi_news import *
from crawler.crawler_tencent_news import *
from concurrent.futures import ProcessPoolExecutor

craler_site = {
        "haokan": Craler_haokan(),
        "toutiao": Craler_toutiao(),
        "腾讯视频": Craler_qq(),
        "new_tudou": Craler_tudou(),
        "网易新闻": Crawler_wangyi_news(),
        "腾讯新闻": Crawler_Tencent_News()
}


def start_count(releaser, platform, releaserUrl, re_s_t, re_e_t):
    craler = craler_site.get(platform)
    if craler:
        url_lis = [
                {
                        "platform": platform,
                        "url": releaserUrl,
                        "releaser": releaser
                }
        ]
        craler.time_range_video_num(re_s_t, re_e_t, url_lis)


if __name__ == "__main__":
    miaopai_list = []
    platform_dic = {
            "haokan": [],
            "toutiao": [],
            "腾讯视频": [],
            "new_tudou": [],
            "网易新闻": [],
            "腾讯新闻":[]
    }
    file = r'count.csv'
    #file = r'D:\wxfile\WeChat Files\litaolemo\FileStorage\File\2019-07\count(3).csv'
    now = int(datetime.datetime.now().timestamp() * 1e3)
    executor = ProcessPoolExecutor(max_workers=6)
    futures = []
    with open(file, 'r', encoding="gb18030")as f:
        header_Lst = f.readline().strip().split(',')
        for line in f:
            line_Lst = line.strip().split(',')
            line_dict = dict(zip(header_Lst, line_Lst))
            releaser = line_dict['releaser']
            platform = line_dict['platform']
            releaserUrl = line_dict['releaserUrl']
            re_s_t = line_dict['开始时间']
            re_e_t = line_dict['结束时间']
            start_time_lis = re_s_t.split("/")
            end_time_lis = re_e_t.split("/")
            start_time_stamp = int(datetime.datetime(year=int(start_time_lis[0]), month=int(start_time_lis[1]),
                                                     day=int(start_time_lis[2])).timestamp() * 1e3)
            end_time__stamp = int(datetime.datetime(year=int(end_time_lis[0]), month=int(end_time_lis[1]),
                                                    day=int(end_time_lis[2])).timestamp() * 1e3)
            # future = executor.submit(start_count, releaser, platform, releaserUrl, start_time_stamp, end_time__stamp)
            # futures.append(future)
            start_count(releaser, platform, releaserUrl, start_time_stamp, end_time__stamp)
    executor.shutdown(True)
    print('+++>完成')
    # for future in futures:
    #     print(future.result())
