# -*- coding: utf-8 -*-
"""
Created on Fri Nov 23 13:48:33 2018

input video url, output video play_count gotten from releaser page

@author: fangyucheng
"""

from elasticsearch import Elasticsearch
from crawler.crawler_sys.utils.trans_format import lst_to_csv
from crawler.crawler_sys.utils import trans_format
from crawler.crawler_sys.site_crawler import crawler_v_qq

absolute_file_path = r"C:\Users\zhouyujiang\安徽第一周数据情况.csv"
task_list = trans_format.str_file_to_lst(absolute_file_path)

result_lst = []
crawler = crawler_v_qq.Crawler_v_qq()
for url in task_list:
    get_data = crawler.video_page(url)
    result_lst.append(get_data)
    print("get data at %s" % url)

bug_releaser_list = []
releaserUrl_lst = []
revised_lst = []
for line in result_lst:
    try:
        if line['releaserUrl'] is not None:
            releaserUrl = line['releaserUrl']
            if releaserUrl not in releaserUrl_lst:
                releaserUrl_lst.append(releaserUrl)
                try:
                    crawler.releaser_page(releaserUrl, output_to_es_raw=True,
                                          es_index='test2', doc_type='12zjbfl',
                                          releaser_page_num_max=1000)
                    print ("get releaser data at %s" % releaserUrl)
                except:
                    bug_releaser_list.append(releaserUrl)
            else:
                pass
        else:
            print("this video %s can't find releaser" % line['url'])
    except:
        print("can't get releaser at %s" % url)

hosts = '192.168.17.11'
port = 80
user_id = 'fangyucheng'
password = 'VK0FkWf1fV8f'
http_auth = (user_id, password)
es_connection = Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)

unsolve_lst = []
result_lst2 =[]

for line in task_list:
    try:
        if type(line) == dict:
            url = line['url']
        elif type(line) == str:
            url = line
        search_body = {"query": {"bool": {"filter": [{"term": {"url.keyword": url}}]}}}
        search = es_connection.search(index="test2", doc_type="12zjbfl", body=search_body)
        if search["hits"]["total"] == 0:
            unsolve_lst.append(url)
            print("can not get video data at %s" % url)
        else:
            video_data = search["hits"]["hits"][0]["_source"]
            result_lst2.append(video_data)
            print("get playcount at %s" % url)
    except:
        pass

lst_to_csv(listname=result_lst2,
           csvname=r"C:\Users\zhouyujiang\12121212121.csv")