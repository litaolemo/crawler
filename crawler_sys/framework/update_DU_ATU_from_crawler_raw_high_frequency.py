# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 16:56:45 2018

@author: hanye

Due to the play count from v_qq video page maybe album play count rather than
video play count, we have to select these video info, not write into
short-video-production but another new index named album-play-count
"""


import datetime
import argparse
from elasticsearch import Elasticsearch
from crawler_sys.framework.es_short_video import bulk_write_short_video

#from crawler_sys.framework.es_short_video import func_search_is_gaopin

from crawler_sys.framework.es_crawler import scan_crawler_raw_index
from crawler_sys.utils.output_results import bulk_write_into_es


parser = argparse.ArgumentParser(description='You can specify a date to process.')
parser.add_argument('-d', '--file_date',
                    help=('Must in isoformat, similar to "2018-06-07". Other '
                          'format will just be ignored.'))
args = parser.parse_args()
if args.file_date is not None:
    try:
        dayT = datetime.datetime.strptime(args.file_date, '%Y-%m-%d')
    except:
        print('Ill format for parameter -t: %s, should be in isoformat, '
              'similar to "2018-06-07". The input parameter is ignored, '
              'will continue to run with default parameters. Ctrl-C to '
              'interrupt or just kill -9 pid.' % args.file_date)
else:
    dayT = datetime.datetime.now()

#dayT = datetime.datetime.today()
fetch_time_start_T = datetime.datetime(dayT.year, dayT.month, dayT.day) - datetime.timedelta(days=1)
# fetch date range spreads on two days rather than one, to
# avoid missing data because task time overlap
fetch_time_end_T = fetch_time_start_T  + datetime.timedelta(days=1)
fetch_time_start_ts = int(fetch_time_start_T.timestamp()*1e3)
fetch_time_end_ts = int(fetch_time_end_T.timestamp()*1e3)

index_target_releaser = 'target_releasers'
doc_type_target_releaser = 'doc'

es_framework = Elasticsearch(hosts='192.168.17.11', port=80,
                             http_auth=('crawler', 'XBcasfo8dgfs'))

def func_search_is_gaopin(releaser, platform):
    search_body_gaopin = {
                     "query": {
                            "bool": {
                              "filter": [
                                {"term": {"platform.keyword": platform}},
                                 {"term": {"releaser.keyword": releaser}}
                                ]
                            }
                            }
                   }
    search_gaopin_re = es_framework.search(index=index_target_releaser, 
                                    doc_type=doc_type_target_releaser,
                                    body=search_body_gaopin)
    if search_gaopin_re['hits']['total'] > 0:
        if search_gaopin_re['hits']['hits'][0]['_source']['frequency'] >= 3:
            return True
        else:
            return False
    else:
        return False
        
        
true_set = set()
false_set = set()
find_data_from_crawler_raw_bd = {
  "query": {
    "bool": {
      "filter": [
        {
          "range": {
            "fetch_time": {
              "gte": fetch_time_start_ts,
              "lt": fetch_time_end_ts
            }
          }
        }
      ],
      "must_not": [
        {"term":{"data_source": "interactioncount"}}
      ]
    }
  }
}


total_hit, scan_resp = scan_crawler_raw_index(find_data_from_crawler_raw_bd)
if total_hit > 0:
    line_counter = 0
    data_Lst = []
    for line in scan_resp:
        line_counter += 1
        line_d = line['_source']
        releaser = line_d['releaser']
        platform = line_d['platform']
        if releaser == None:
            continue
        if platform+releaser in true_set:
            data_Lst.append(line_d)
        else:
            if func_search_is_gaopin(releaser, platform):
                true_set.add(platform+releaser)
                data_Lst.append(line_d)
            else:
                false_set.add(platform+releaser)
        if line_counter%1000==0 or line_counter==total_hit:
            print('Writing lines %d/%d into short video index, %s'
                  % (line_counter, total_hit, datetime.datetime.now()))
            bulk_write_short_video(data_Lst,
                                   #index='test_write6', # test
                                   )
            data_Lst.clear()

    if data_Lst != []:
        print('Writing lines %d/%d into short video index, %s'
              % (line_counter, total_hit, datetime.datetime.now()))
        bulk_write_short_video(data_Lst,
                               #index='test_write6', # test
                               )
        data_Lst.clear()
    print('All done. %s' % datetime.datetime.now())
else:
    print('Zero hit, program exits. %s' % datetime.datetime.now())

#for those video info with album play count, write them into another es index
find_album_play_count_data = {
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "data_source": "interactioncount"
          }
        },
        {
          "range": {
            "fetch_time": {
              "gte": fetch_time_start_ts,
              "lt": fetch_time_end_ts
            }
          }
        }
      ]
    }
  }
}

total_hit, scan_resp = scan_crawler_raw_index(find_album_play_count_data)
if total_hit > 0:
    line_counter = 0
    album_play_count_lst = []
    for line in scan_resp:
        line_counter += 1
        line_d = line['_source']
        album_play_count_lst.append(line_d)
        if line_counter%1000==0 or line_counter==total_hit:
            print('Writing lines %d/%d into index album-play-count, %s'
                  % (line_counter, total_hit, datetime.datetime.now()))
            bulk_write_into_es(dict_Lst=album_play_count_lst,
                               index='album-play-count',
                               doc_type='doc')
            album_play_count_lst.clear()

    if album_play_count_lst != []:
        print('Writing lines %d/%d index album-play-count, %s'
              % (line_counter, total_hit, datetime.datetime.now()))
        bulk_write_into_es(dict_Lst=album_play_count_lst,
                           index='album-play-count',
                           doc_type='doc')
        album_play_count_lst.clear()
    print('write album play count into another index. %s' % datetime.datetime.now())
else:
    print('Zero hit, program exits. %s' % datetime.datetime.now())
    
