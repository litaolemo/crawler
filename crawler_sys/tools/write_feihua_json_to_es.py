import requests
import json
import datetime
import elasticsearch
from write_data_into_es.func_cal_doc_id import *
hosts = '192.168.17.11'
port = 80
user = 'zhouyujiang'
passwd = '8tM9JDN2LVxM'
http_auth = (user, passwd)
es = elasticsearch.Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)
from write_data_into_es.func_get_releaser_id import get_releaser_id

def dic():
    url_data = []
    format_data = {}
    f = open("E:\M_2019-05-22_U_3", "r")
    for i in f.readlines():
        url_data.append(json.loads(i))
    for i in url_data:
        format_data[i["url"]] = ""
    print(len(format_data))

def get_data():
    url_data = []
    format_data = []
    # url = "https://enlightent-backup.oss-cn-beijing.aliyuncs.com/csm/20190318-20190324/csm_douyin_20190318_20190324.json?Expires=1554243367&OSSAccessKeyId=TMP.AQG2JUd3g4Gv66npoCNJPVnH-r9yRqhMGwqJtilxiBCDsbRJJ4kTuiE_T17CMC4CFQC8gXq7WHE73SSE9s2DjpWzF7Y2TwIVAIeJz9r0QHkaPi8FGyzN1TXmsjvn&Signature=XsHnMu%2B4agHS6Z6tq%2B55WWaZjDk%3D"
    # res = requests.get(url)
    # with open("./url_json.json","w") as f:
    #     f.write(res.text)

    # f = open("E:\M_2019-05-22_U_3", "r")
    # for i in f.readlines():
    #     url_data.append(json.loads(i))
    for i in url_data:
        print(i)
        format_data.append(
            {
                "platform": i["platform"],
                "duration": i["duration"],
                "favorite_count": i["favorite"],
                "fetch_time": int(i["crawledtime"])*1000,
                "play_count": i["playtimes"],
                "release_time": i["releasetime"],
                "releaser": i["releaser"],
                "title": i["title"],
                "url": i["url"],
                "comment_count": i["commentnum"],
                "dislike_count": 0,
                "isOriginal": False,
                "releaserUrl": i["releaserurl"],
                "repost_count": 0,
                "timestamp": int(datetime.datetime.timestamp(datetime.datetime.now()))*1000,
                "data_provider": "fhtech",
                "channel": i["channel"],
                "releaser_id_str":"miaopai_" + get_releaser_id(platform="miaopai",releaserUrl=i["releaserurl"])
            }
        )
    return format_data
# target_date_list = target_type.split('-')
# target_date_start = datetime.datetime(int(target_date_list[-3]), int(target_date_list[-2]), 1)
# target_date_end = datetime.datetime(int(target_date_list[-3]), int(target_date_list[-2]) + 1, 1)
# target_ts_start = int(target_date_start.timestamp()) * 1000
# target_ts_end = int(target_date_end.timestamp()) * 1000
# print(target_ts_start)
# print(target_ts_end)


def write_es(file):
    count = 0
    bulk_all_body = ""
    doc_id_type = "all-time-url"
    for i in file:
        #print(i)
        # format_i = {}
        # a = "format_i = %s" % i
        # exec(a,format_i)
        format_i = json.loads(i)
        # format_i = format_i["format_i"]
        # print(format_i)
        try:
            _id = cal_doc_id(platform=format_i["platform"], url=format_i["url"], doc_id_type=doc_id_type,data_dict=format_i)
            format_i["timestamp"] = int(datetime.datetime.now().timestamp()*1e3)
            if len(str(format_i["release_time"])) != 13:
                print(format_i["release_time"])
                format_i["release_time"] = int(format_i["release_time"] / 1000)
            format_i["releaser_id_str"] = "miaopai_" + get_releaser_id(platform="miaopai",releaserUrl=format_i["releaserUrl"])
            bulk_head = '{"index": {"_id":"%s"}}' % _id
        except Exception as e:
            print(e)
            continue
        # find_exist = {
        #     "query": {
        #         "bool": {
        #             "filter": [
        #                 {"term": {"_id":_id }}
        #             ]
        #         }
        #     }
        # }
        # search_re = es.search(index=target_index, doc_type=target_type,
        #                       body=find_exist)
        # if search_re['hits']['total'] == 1:
        #     if counti % 1000 == 0:
        #         print("done ", counti,"\n")
        #         return None
        # else:
        #     pass

        data_str = json.dumps(format_i, ensure_ascii=False)
        bulk_one_body = bulk_head + '\n' + data_str + '\n'
        bulk_all_body += bulk_one_body
        count += 1
        print("find",count)
        if count % 1000 == 0:
            eror_dic = es.bulk(index=target_index, doc_type=target_type,
                               body=bulk_all_body, request_timeout=200)
            bulk_all_body = ''
            if eror_dic['errors'] is True:
                print(eror_dic['items'])
                print(bulk_all_body)
            print(count)

    if bulk_all_body != '':
        eror_dic = es.bulk(body=bulk_all_body,
                           index=target_index,
                           doc_type=target_type,
                           request_timeout=200)
        if eror_dic['errors'] is True:
            print(eror_dic)
            bulk_all_body = ''
            #print(platform, releaser, 'end_have:', len(wirte_set), 'add:', len(set_url))


if __name__ == '__main__':
    target_index = 'short-video-all-time-url'
    target_type = 'all-time-url'
    m3 = open(r"C:\Users\litao\Desktop\csv\202002\M_2020-02-04_U_3", "r", encoding="utf-8")
    # f = open("exists", "a+")
    write_es(m3)
