# -*- coding: utf-8 -*-
"""
Created on Tue Sep 18 21:34:48 2018

@author: fangyucheng
"""

"""
get data from es.crawler_data_raw, platform is toutiao
to test whether the playcount from video page is same as 
playcount from releaser page
"""

import re
from elasticsearch import Elasticsearch
from crawler_sys.framework.get_redirect_resp_proxy import get_redirected_resp
from crawler_sys.proxy_pool import connect_with_database

hosts = '192.168.17.11'
port = 80
user_id = 'fangyucheng'
password = 'VK0FkWf1fV8f'
http_auth = (user_id, password)
lose_re_url = []
es = Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)

search_url = {"query": {"bool": {"filter": [{"term":{"platform.keyword":"toutiao"}},
                                            {"range":{"fetch_time":{"gte":1537200000000}}}]}}}
try:
    search_url_re = es.search(index='short-video-production',
                              doc_type='daily-url',
                              body=search_url,
                              request_timeout=100,
                              size=1000)
    search_lst = search_url_re['hits']['hits']
except:
    print("can't extract data from es")
    pass

video_info_lst = []
for line in search_lst:
    video_info = line['_source']
    video_info_lst.append(video_info)

proxy_lst = connect_with_database.extract_data_to_use()

for line in proxy_lst:
    line['proxy_dic'] = {line['category']: line['whole_ip_address']}


proxy_dic = proxy_lst[0]['proxy_dic']

for line in video_info_lst:
    url = line['url']
    if "toutiao.com" in url:
        video_id_str = ' '.join(re.findall('/group/[0-9]+', url))
        video_id = ' '.join(re.findall('\d+', video_id_str))
        url = 'http://www.365yg.com/a' + video_id
    get_page = get_redirected_resp(url, proxy_dic)
    if get_page == None:
        print("can't get page")
    else:
        page = get_page.text
        find_play_count = re.findall('videoPlayCount: \d+,', page)
        if find_play_count != []:
            play_count = re.findall('\d+', find_play_count[0])[0]
            line['play_video_from_video_page'] = play_count
        else:
            print("can't get play_count")