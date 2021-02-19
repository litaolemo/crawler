# -*- coding:utf-8 -*-
# @Time : 2019/7/19 11:29
# @Author : litao

import json
import datetime
import elasticsearch
import hashlib
import csv
hosts = '192.168.17.11'
port = 80
user = 'zhouyujiang'
passwd = '8tM9JDN2LVxM'
http_auth = (user, passwd)
es = elasticsearch.Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)


def write_es(Lst):
    count = 0
    bulk_all_body = ""
    doc_id_type = "all-time-url"
    header_Lst = Lst[0]
    linec = 1
    sha1 = hashlib.sha1()
    for line in Lst:
        if linec == 1:
            linec += 1
            continue
        linec += 1
        print(linec)
        line_dict = dict(zip(header_Lst, line))
        dic = {
                "title": line_dict["title"],
                "timestamp": int(datetime.datetime.now().timestamp() * 1e3),
                "platform": line_dict["platform"],
                "page": line_dict["page"],
        }

        sha1.update((line_dict["title"]+line_dict["platform"]).encode("utf8"))
        bulk_head = '{"index": {"_id":"%s"}}' % sha1.hexdigest()
        data_str = json.dumps(dic, ensure_ascii=False)
        bulk_one_body = bulk_head + '\n' + data_str + '\n'
        bulk_all_body += bulk_one_body
        count += 1
        if count % 500 == 0:
            eror_dic = es.bulk(index=target_index, doc_type=target_type,
                               body=bulk_all_body, request_timeout=500)
            bulk_all_body = ''
            if eror_dic['errors'] is True:
                print(eror_dic['items'])
                print(bulk_all_body)
            print(count)

    if bulk_all_body != '':
        eror_dic = es.bulk(body=bulk_all_body,
                           index=target_index,
                           doc_type=target_type,
                           request_timeout=500)
        if eror_dic['errors'] is True:
            print(eror_dic)
            bulk_all_body = ''
            # print(platform, releaser, 'end_have:', len(wirte_set), 'add:', len(set_url))


if __name__ == '__main__':
    target_index = 'search_keywords'
    target_type = 'doc'

    m3 = open(r"D:\work_file\发布者账号\一次性需求附件\keywords.csv", "r", encoding="gb18030")
    file = csv.reader(m3)
    data = list(file)
    write_es(data)
