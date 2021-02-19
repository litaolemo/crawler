# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 16:56:45 2018

@author: hanye

Due to the play count from v_qq video page maybe album play count rather than
video play count, we have to select these video info, not write into
short-video-production but another new index named album-play-count
"""

import os
import re
import datetime
import argparse
import configparser
from elasticsearch.helpers import scan
from elasticsearch import Elasticsearch
from crawler_sys.framework.es_short_video import bulk_write_short_video
from crawler_sys.framework.es_crawler import scan_crawler_raw_index
from crawler_sys.utils.output_results import bulk_write_into_es


es_framework = Elasticsearch(hosts='192.168.17.11', port=80,
                             http_auth=('crawler', 'XBcasfo8dgfs'))

parser = argparse.ArgumentParser(description='You can specify a date to process.')
parser.add_argument('-d', '--file_date',
                    help=('Must in isoformat, similar to "2018-06-07". Other '
                          'format will just be ignored.'))
parser.add_argument('-H', '--write_high',
                    help=('like 1 OR 0'),
                    default=1)
parser.add_argument('-R', '--write_day',
                    help=('like 1 OR 0'),
                    default=1)
parser.add_argument('-p', '--target_platform', action='append', default=None,
                    help=('Write sigle platform.similaer to "抖音"'))
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

def save_log(process):
    with open("/home/hanye/crawlersNew/crawler/crawler_log/daily_log", "a", encoding="utf-8") as f:
        f.write(process + "_at_" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n")


save_log("start")
high_fre_index = "target_releasers_org"
# get legal platforms from configure file
#查找当前目录
#cwd = os.getcwd()

#if os.name == 'nt':
#    path_sep = '\\'
#    pattern = '.+\\\\crawler\\\\'
#else:
#    path_sep = '/'
#    pattern = '.+/crawler/'
#parent_pth = re.findall(pattern, cwd)[0]
config_folder_pth_relative = '/crawler_sys/framework/config'
parent_pth = '/home/hanye/crawlersNew/crawler'
config_folder_pth_abs = parent_pth + config_folder_pth_relative
legal_platforms_config_fn = 'legal_platforms.ini'
config = configparser.ConfigParser()
with open(config_folder_pth_abs + '/' + legal_platforms_config_fn,
          'r', encoding='utf-8') as conf_file:
    config.read_file(conf_file)
legal_platforms = config['legal_platforms_to_update_production'
                      ]['legal_platforms'].split(',')
print(legal_platforms)
#dayT = datetime.datetime.today()
fetch_time_start_T = datetime.datetime(dayT.year, dayT.month, dayT.day) + datetime.timedelta(days=-1)
# fetch date range spreads on two days rather than one, to
# avoid missing data because task time overlap
fetch_time_end_T = fetch_time_start_T + datetime.timedelta(days=2)
fetch_time_start_ts = int(fetch_time_start_T.timestamp()*1e3)
fetch_time_end_ts = int(fetch_time_end_T.timestamp()*1e3)

release_time_start_T = fetch_time_start_T - datetime.timedelta(days=60)
release_time_start_ts = int(release_time_start_T.timestamp()*1e3)

fetch_time_start_T_high = datetime.datetime(dayT.year, dayT.month, dayT.day) - datetime.timedelta(days=2)
fetch_time_end_T_high = fetch_time_start_T_high + datetime.timedelta(days=1)
fetch_time_start_ts_high = int(fetch_time_start_T_high.timestamp()*1e3)
fetch_time_end_ts_high = int(fetch_time_end_T_high.timestamp()*1e3)

if args.target_platform:
    legal_platforms = args.target_platform

if args.write_day == 1:
    print('start write low into production')
    for platform in legal_platforms:
        print(platform)
        find_data_from_crawler_raw_bd = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"platform.keyword": platform}},
                        {"range":
                            {"fetch_time":
                                {"gte": fetch_time_start_ts,
                                 "lt": fetch_time_end_ts
                                }
                            }
                        },
                        {"range":
                            {"release_time":
                                {"gte": release_time_start_ts}
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
                data_Lst.append(line_d)
                if line_counter%500==0 or line_counter==total_hit:
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


scan_high_releaser = []
save_log("end_time_write_to_alltime")
if args.write_high == 1:
    print('start write high into production')
    high_re_list = []
    high_count = 0
    search_high_releaser_body = {
                  "query": {
                        "bool": {
                          "filter": [
                             {"range": {"frequency": {"gte": 3}}}
                            ]
                        }
                      }
                       }
    scan_high_1 = scan(client=es_framework,
                                          index=high_fre_index,
                                          doc_type='doc',
                                          query=search_high_releaser_body)
    scan_high_2 = scan(client=es_framework,
                       index="target_releasers",
                       doc_type='doc',
                       query=search_high_releaser_body)
    print('start  frequency releaser')
    for res in scan_high_1:
        scan_high_releaser.append(res)
    for res in scan_high_2:
        if res not in scan_high_releaser:
            scan_high_releaser.append(res)
    print("get %s releaser in high frequency" % len(scan_high_releaser))
    # write high frequency releaser craw data in yesterday
    for one_high_releaser in scan_high_releaser:
        platform = one_high_releaser['_source']['platform']
        if platform in legal_platforms:
            try:
                releaser = one_high_releaser['_source']['releaser']
                releaser_id_str = one_high_releaser['_source'].get('releaser_id_str')
                scan_high_releaser_body = {
                                "query": {
                                    "bool": {
                                        "filter": [
                                            {"term": {"platform.keyword": platform}},

                                            {"range":
                                                {"fetch_time":
                                                    {"gte": fetch_time_start_ts_high,
                                                     "lt": fetch_time_end_ts_high
                                                    }
                                                }
                                            },
                                            {"range":
                                                {"release_time":
                                                    {"gte": release_time_start_ts}
                                                }
                                            }
                                        ],
                                        "must_not": [
                                            {"term":{"data_source": "interactioncount"}}
                                        ]
                                    }
                                }
                            }
                if releaser_id_str:
                    scan_high_releaser_body["query"]["bool"]["filter"].append({"term": {"releaser_id_str.keyword": releaser_id_str}})
                else:
                    scan_high_releaser_body["query"]["bool"]["filter"].append(
                            {"term": {"releaser.keyword": releaser}})

                total_one_releaser, total_high_data = scan_crawler_raw_index(scan_high_releaser_body)
                if total_one_releaser != 0:
                    for one_high_data in total_high_data:
                        high_count = high_count + 1
                        high_re_list.append(one_high_data['_source'])
                        if high_count % 500 == 0:
                            print('Writing lines %d into short video index, %s %s %s'
                              % (high_count, datetime.datetime.now(), platform, releaser))
                            bulk_write_short_video(high_re_list)
                            high_re_list.clear()
            except Exception as e:
                print(e)
                print('wrong in ', platform, releaser)
        else:
            print('platform is not allowed to writeinto production:',platform)
    
    if high_re_list != []:
        print('Writing lines %d into short video index, %s'
                          % (high_count, datetime.datetime.now()))
        bulk_write_short_video(high_re_list)
        high_re_list.clear()
save_log("end_high_frequency_")
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
        if line_counter%500==0 or line_counter==total_hit:
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

save_log("end_time_")