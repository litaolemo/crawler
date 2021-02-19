# -*- coding: utf-8 -*-
"""
Created on Wed Jun 13 11:57:40 2018

@author: fangyucheng
"""

import elasticsearch
import json
import time
from crawler_sys.utils.releaser_url_check import test_releaserUrl
from crawler_sys.utils import trans_format


hosts = '192.168.17.11'
port = 80
user_id = 'fangyucheng'
password = 'VK0FkWf1fV8f'
http_auth = (user_id, password)
lose_re_url = []
es = elasticsearch.Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)

test_lst = trans_format.csv_to_lst_with_headline('F:/add_target_releaser/album_playcnt/album_playcnt_002.csv')
task_lst = []

for line in test_lst:
    if line['releaserUrl'] is not None:
        task_lst.append(line)

bulk_all_body = ''

poster = 'fangyucheng'
test_re = test_releaserUrl(task_lst)

for one_re in test_re:
    if  one_re['True_or_False'] == 1:
        line_dic = {}
        post_by = poster
        post_time = int(time.time() * 1000)
        timestamp = int(time.time() * 1000)
        releaserUrl = one_re['releaserUrl']
        platform = one_re['platform']
        releaser = one_re['releaser']
        try:
            album_play_count = one_re['album_play_count']
        except:
            album_play_count = None
        _id = platform + '_' + releaser

        bulk_head = '{"index": {"_id":"%s"}}' % _id
        line_dic['is_valid'] = True
        line_dic['platform'] = platform
        line_dic['post_by'] = post_by
        if album_play_count is not None:
            line_dic['album_play_count'] = album_play_count
        line_dic['post_time'] = post_time
        line_dic['releaser'] = releaser
        line_dic['releaserUrl'] = releaserUrl
        line_dic['timestamp'] = timestamp
        data_str=json.dumps(line_dic, ensure_ascii=False)
        bulk_one_body = bulk_head + '\n' + data_str + '\n'
        bulk_all_body += bulk_one_body
        es.bulk(index='target_releasers', doc_type='doc',
                body=bulk_all_body, request_timeout=200)
        bulk_all_body = ''
        print('success')

