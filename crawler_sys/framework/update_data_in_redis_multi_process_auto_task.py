# -*- coding:utf-8 -*-
# @Time : 2019/9/10 15:44 
# @Author : litao
import redis,time,json
from crawler.crawler_sys.site_crawler_test import (crawler_toutiao,crawler_v_qq,crawler_tudou,crawler_haokan,
                                                   crawler_tencent_news,
                                                   crawler_wangyi_news,crawler_kwai,crawler_douyin)
from maintenance.send_email_with_file_auto_task import write_email_task_to_redis
import sys
import argparse, copy,datetime
from multiprocessing import Pool
from qingboAPI.reback_data_api import reback_data,get_biz_from_url
# from crawler.crawler_sys.framework.platform_crawler_register import get_crawler
from write_data_into_es.func_get_releaser_id import get_releaser_id

rds = redis.StrictRedis(host='192.168.17.60', port=6379, db=2,decode_responses=True)
# 爬虫补抓系统

platform_crawler_reg = {
    'toutiao': crawler_toutiao.Crawler_toutiao,
    '腾讯视频': crawler_v_qq.Crawler_v_qq,
    # 'iqiyi': crawler_iqiyi.Crawler_iqiyi,
    # 'youku': crawler_youku.Crawler_youku,
    'new_tudou': crawler_tudou.Crawler_tudou,
    'haokan': crawler_haokan.Crawler_haokan,
    '腾讯新闻': crawler_tencent_news.Crawler_Tencent_News,
    # 'miaopai': crawler_miaopai.Crawler_miaopai,
    # 'pearvideo': crawler_pear.Crawler_pear,
    # 'bilibili': crawler_bilibili.Crawler_bilibili,
    # 'Mango': crawler_mango,
    "网易新闻": crawler_wangyi_news.Crawler_wangyi_news,
    # "kwai": crawler_kwai.Crawler_kwai,
    # "抖音": crawler_douyin.Crawler_douyin
}

def get_crawler(platform):
    if platform in platform_crawler_reg:
        platform_crawler = platform_crawler_reg[platform]
    else:
        platform_crawler = None
        print("can't get crawler for platform %s, "
              "do we have the crawler for that platform?" % platform)
    return platform_crawler

def get_crawler_list_from_redis(project):
    data_lis = []
    one_project_dic = rds.hgetall(project)
    crawler_dur = int(one_project_dic.get("duration"))
    data = json.loads(one_project_dic.get("data"))
    try:
        email_dic = json.loads(one_project_dic.get("email"))
    except:
        email_dic = None
    for d in data:
        platform,releaserUrl = d.split("&",1)
        data_lis.append({"platform":platform,"releaserUrl":releaserUrl})
    return data_lis,crawler_dur,email_dic


def get_project_name_from_redis():
    res = rds.llen("project")
    if res != 0:
        one_project_name = rds.lpop("project")
        crawler_lis,crawler_duration,email_dic = get_crawler_list_from_redis(one_project_name)
        return crawler_lis,crawler_duration,one_project_name,email_dic
    else:
        return None,None,None,None

def delete_project_form_redis(one_project_name,email_dic):
    rds.delete(one_project_name)
    rds.hset("task_down",one_project_name,int(datetime.datetime.now().timestamp()*1e3))
    if email_dic:
        write_email_task_to_redis(task_name=one_project_name,email_group=email_dic.get("email_group"),email_msg_body_str=email_dic.get("email_msg_body_str"),title_str=email_dic.get("title_str"),cc_group=email_dic.get("cc_group"),sender=email_dic.get("sender"))

def start_crawler(crawler_lis, crawler_duration,email_dic,processes=15):
    pool = Pool(processes=processes)
    start_time = int(crawler_duration)
    end_time = int(datetime.datetime.now().timestamp() * 1e3)
    kwargs_dict = {
            'output_to_file': False,
            'filepath': "",
            'releaser_page_num_max': 10000,
            'output_to_es_raw': True,
            'es_index': "crawler-data-raw",
            'doc_type': "doc",
            'output_to_es_register': False,
            'push_to_redis': False,
            "proxies_num":3
    }
    for one_data in crawler_lis:
        platform = one_data.get("platform")
        releaserUrl = one_data.get("releaserUrl")
        # 3 get crawler for this platform
        Platform_crawler = get_crawler(platform)
        # print(releaserUrl_Lst)

        if platform in ["weixin"]:
            res = reback_data(platform=platform,releaser_id=get_biz_from_url(releaserUrl),start_time=datetime.datetime.fromtimestamp(start_time/1e3).strftime("%Y-%m-%d %H:%M:%S"),
                        end_time=datetime.datetime.fromtimestamp(end_time/1e3).strftime("%Y-%m-%d %H:%M:%S"))
            email_dic["email_msg_body_str"] += "\n" + platform + " " +releaserUrl + " 提交清博接口回溯" + "成功" if res else "失败"
        elif platform in ["miaopai", "kwai", "抖音"]:
            res = reback_data(platform=platform, releaser_id=get_releaser_id(releaserUrl),
                        start_time=datetime.datetime.fromtimestamp(start_time / 1e3).strftime("%Y-%m-%d %H:%M:%S"),
                        end_time=datetime.datetime.fromtimestamp(end_time / 1e3).strftime("%Y-%m-%d %H:%M:%S"))
            email_dic["email_msg_body_str"] += "\n" + platform + " " + releaserUrl + " 提交清博接口回溯" + "成功" if res else "失败"

        elif Platform_crawler != None:
            crawler_instant = Platform_crawler()
            crawler = crawler_instant.releaser_page_by_time
        else:
            print('Failed to get crawler for platform %s' % platform)
            continue
        # 4 for each releaserUrl, get data on the releaser page identified by this
        # releaserUrl, with multiprocesses
        pool.apply_async(func=crawler, args=(start_time, end_time, releaserUrl), kwds=kwargs_dict)
    pool.close()
    pool.join()
    print('Multiprocessing done')
    return email_dic


if __name__ == "__main__":
    # rds.flushdb()
    now = datetime.datetime.now()
    while True and now.hour >= 5:
        crawler_lis, crawler_duration,one_project_name,email_dic = get_project_name_from_redis()
        if crawler_lis and crawler_duration:
            try:
                new_process = start_crawler
                print(crawler_lis)
                email_dic = new_process(crawler_lis, crawler_duration,email_dic)
            except:
                pass
            delete_project_form_redis(one_project_name,email_dic)
        else:
            print("wait for 5s")
            now = datetime.datetime.now()
            time.sleep(5)
    sys.exit(0)


