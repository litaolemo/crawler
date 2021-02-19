# -*- coding:utf-8 -*-
# @Time : 2020/3/5 15:34 
# @Author : litao
import datetime
from crawler.crawler_sys.hot_words_crawler.crawler_wangyi_news import Crawler_WangYi_News
from crawler.crawler_sys.hot_words_crawler.crawler_v_qq import Crawler_v_qq
from crawler.crawler_sys.hot_words_crawler.crawler_haokan import CrawlerHaoKan
from crawler.crawler_sys.hot_words_crawler.crawler_douyin import Crawler_douyin
from crawler.crawler_sys.hot_words_crawler.crawler_qq_news import Crawler_Qq_News
from crawler.crawler_sys.hot_words_crawler.crawler_new_tudou import CrawlerNewTudou
from crawler.crawler_sys.hot_words_crawler.crawler_toutiao import Crawler_toutiao
from crawler.crawler_sys.hot_words_crawler.crawler_kwai import Crawler_kwai

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

es = Elasticsearch(hosts='192.168.17.11', port=80,
                   http_auth=('crawler', 'XBcasfo8dgfs'))

now = int(datetime.datetime.now().timestamp() * 1e3) - 86400000
platform_dic = {
        "kwai": Crawler_kwai(),
        "toutiao": Crawler_toutiao(),
        "haokan": CrawlerHaoKan(),
        "抖音": Crawler_douyin(),
        "腾讯视频": Crawler_v_qq(),
        "腾讯新闻": Crawler_Qq_News(),
        "new_tudou": CrawlerNewTudou(),
        "网易新闻": Crawler_WangYi_News()
}

for platform in platform_dic:
    res = platform_dic[platform].get_hot_words()
    print(platform, res)

search_body = {
        "query": {
                "bool": {
                        "filter": [
                                # {"term":{"platform.keyword":"腾讯视频"}},
                                {"range": {"fetch_time": {"gte": now}}}
                        ]
                }
        }
}
keyword_scan = scan(client=es, index='short-video-hotwords', query=search_body, doc_type="doc")
res_list = []
for res in keyword_scan:
    res_list.append(res["_source"])

for res in res_list:
    try:
        platform_dic[res["platform"]].get_hot_videos(**res)
    except Exception as e:
        print(res["platform"], e)
        continue
