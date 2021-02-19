# -*- coding: utf-8 -*-
"""
Created on Tue May 15 13:59:43 2018

@author: hanye
"""

import json
import datetime
import time
import re
import requests
from elasticsearch.exceptions import TransportError
from crawler_sys.framework.redis_interact import feed_url_into_redis
from crawler_sys.framework.redis_interact import rds
from crawler_sys.framework.es_ccr_index_defination import es_framework as es_site_crawler
from crawler_sys.framework.es_ccr_index_defination import index_url_register
from crawler_sys.framework.es_ccr_index_defination import doc_type_url_register
from crawler_sys.framework.es_ccr_index_defination import fields_url_register
from crawler_sys.framework.es_crawler import construct_id_for_url_register
from crawler_sys.utils.write_into_file import write_str_into_file
from crawler.crawler_sys.proxy_pool.func_get_proxy_form_kuaidaili import get_proxy

index_site_crawler = 'crawler-data-raw'
doc_type_site_crawler = 'doc'

def form_data_Lst_for_url_register(data_Lst_ori):
    ts = int(datetime.datetime.now().timestamp()*1e3)
    data_Lst_reg = []
    for line in data_Lst_ori:
        try:
            fields_ori = set(line.keys())
            fields_rem = set(fields_url_register)
            fields_to_remove = fields_ori.difference(fields_rem)
            for field in fields_to_remove:
                line.pop(field)
            line['timestamp'] = ts
            data_Lst_reg.append(line)
        except:
            print('attributeerror at %s' % data_Lst_ori.index(line))
    return data_Lst_reg


def hot_words_output_result(result_Lst,output_index="short-video-hotwords",output_doc="doc"):
    bulk_all_body = ""
    for count,result in enumerate(result_Lst):
        doc_id = result["platform"] + "_"+ result["title"]
        bulk_head = '{"index": {"_id":"%s"}}' % doc_id
        data_str = json.dumps(result, ensure_ascii=False)
        bulk_one_body = bulk_head + '\n' + data_str + '\n'
        bulk_all_body += bulk_one_body
        if count % 500 == 0 and count >0:

            eror_dic = es_site_crawler.bulk(index=output_index, doc_type=output_doc,
                               body=bulk_all_body, request_timeout=200)
            bulk_all_body = ''
            if eror_dic['errors'] is True:
                print(eror_dic['items'])
                print(bulk_all_body)
            print(count)


    if bulk_all_body != '':
        eror_dic = es_site_crawler.bulk(body=bulk_all_body,
                           index=output_index,
                           doc_type=output_doc,
                           request_timeout=200)
        if eror_dic['errors'] is True:
            print(eror_dic)

def output_result(result_Lst, platform,
                  output_to_file=False, filepath=None,
                  output_to_es_raw=False,
                  output_to_es_register=False,
                  push_to_redis=False,
                  batch_str=None,
                  release_time_lower_bdr=None,
                  es_index=index_site_crawler,
                  doc_type=doc_type_site_crawler):
    # write data into es crawler-raw index
    if output_to_es_raw:
        bulk_write_into_es(result_Lst, es_index, doc_type)

    # write data into es crawler-url-register index
    if output_to_es_register:
        data_Lst_reg = form_data_Lst_for_url_register(result_Lst)
        bulk_write_into_es(data_Lst_reg,
                           index=index_url_register,
                           doc_type=doc_type_url_register,
                           construct_id=True,
                           platform=platform
                          )

    # feed url into redis
    if push_to_redis:
        redis_list_name, url_counter = feed_url_into_redis(
            result_Lst, platform,
            batch_str=batch_str,
            release_time_lower_bdr=release_time_lower_bdr)

    # output into file according to passed in parameters
    if output_to_file is True and filepath is not None:
        output_fn = ('crawler_%s_on_%s_json'
                     % (platform, datetime.datetime.now().isoformat()[:10]))
        output_f = open(filepath+'/'+output_fn, 'a', encoding='utf-8')
        write_into_file(result_Lst, output_f)
        output_f.close()
    else:
        return result_Lst


def retry_get_url(url, retrys=3, proxies=None,timeout=10,**kwargs):
    retry_c = 0
    while retry_c < retrys:
        try:
            if proxies:
                proxies_dic = get_proxy(proxies)
                if not proxies_dic:
                    get_resp = requests.get(url, timeout=timeout,**kwargs)
                else:
                    get_resp = requests.get(url, proxies=proxies_dic,timeout=timeout, **kwargs)
            else:
                get_resp = requests.get(url, timeout=timeout,**kwargs)
            return get_resp
        except Exception as e:
            retry_c += 1
            time.sleep(1)
            print(e)
    print('Failed to get page %s after %d retries, %s'
          % (url, retrys, datetime.datetime.now()))
    return None


def get_ill_encoded_str_posi(UnicodeEncodeError_msg):
    posi_nums = []
    get_err_posi = re.findall('\s+[0-9]+-[0-9]+:', UnicodeEncodeError_msg)
    if get_err_posi != []:
        posi = get_err_posi[0].replace(' ', '').replace(':', '').split('-')
        for pp in posi:
            try:
                ppn = int(pp)
                posi_nums.append(ppn)
            except:
                pass
    else:
        pass
    return posi_nums

def bulk_write_into_es(dict_Lst,
                       index,
                       doc_type,
                       construct_id=False,
                       platform=None):
    bulk_write_body = ''
    write_counter = 0

    def bulk_write_with_retry_UnicodeEncodeError(bulk_write_body,
                                                 retry_counter_for_UnicodeEncodeError):
        if bulk_write_body != '':
            try:
                bulk_write_resp = es_site_crawler.bulk(body=bulk_write_body,
                                                       request_timeout=100)
                bulk_write_body = ''
#                print(bulk_write_resp)
                print('Writing into es done')
            except UnicodeEncodeError as ue:
                print('Got UnicodeEncodeError, will remove ill formed string and retry.')
                retry_counter_for_UnicodeEncodeError += 1
                UnicodeEncodeError_msg = ue.__str__()
                ill_str_idxs = get_ill_encoded_str_posi(UnicodeEncodeError_msg)
                if len(ill_str_idxs) == 2:
                    ill_str = bulk_write_body[ill_str_idxs[0]: ill_str_idxs[1]+1]
                    bulk_write_body = bulk_write_body.replace(ill_str, '')
                    bulk_write_with_retry_UnicodeEncodeError(bulk_write_body,
                                                             retry_counter_for_UnicodeEncodeError
                                                            )
            except TransportError:
                print("output to es register error")
                write_str_into_file(file_path='/home/fangyucheng/',
                                    file_name='debug',
                                    var=bulk_write_body)
        return retry_counter_for_UnicodeEncodeError

    for line in dict_Lst:
        write_counter += 1
        if construct_id and platform is not None:
            doc_id = construct_id_for_url_register(platform, line['url'])
            action_str = ('{ "index" : { "_index" : "%s", "_type" : "%s", "_id" : "%s" } }'
                          % (index, doc_type, doc_id))
        else:
            action_str = ('{ "index" : { "_index" : "%s", "_type" : "%s" } }'
                          % (index, doc_type))
        data_str = json.dumps(line, ensure_ascii=False)
        line_body = action_str + '\n' + data_str + '\n'
        bulk_write_body += line_body
        if write_counter%1000 == 0 or write_counter == len(dict_Lst):
            print('Writing into es %s/%s %d/%d' % (index, doc_type,
                                                   write_counter,
                                                   len(dict_Lst)))
            if bulk_write_body != '':
                retry_counter_for_UnicodeEncodeError = 0
                retry_counter_for_UnicodeEncodeError = bulk_write_with_retry_UnicodeEncodeError(
                    bulk_write_body,
                    retry_counter_for_UnicodeEncodeError)
                bulk_write_body = ''


def write_into_file(dict_Lst, file_obj):
    for data_dict in dict_Lst:
        json_str = json.dumps(data_dict)
        file_obj.write(json_str)
        file_obj.write('\n')
        file_obj.flush()


def load_json_file_into_dict_Lst(filename, path):
    if path[-1] != '/':
        path += '/'
    data_Lst = []
    with open(path+filename, 'r', encoding='utf-8') as f:
        for line in f:
            line_d = json.loads(line)
            if 'data_provider' not in line_d:
                line_d['data_provider'] = 'CCR'
            if 'releaser_id' in line_d:
                try:
                    line_d['releaser_id'] = int(line_d['releaser_id'])
                except:
                    line_d.pop('releaser_id')
            data_Lst.append(line_d)
    return data_Lst


def crawl_a_url_and_update_redis(url, platform, urlhash, processID=-1):
    # find crawler
    # perform crawling, get the data
    # write es or output to files
    # update redis
    ts = int(datetime.datetime.now().timestamp()*1e3)
    redis_hmset_dict = {'push_time': ts, 'is_fetched': 1,
                        'url': url, 'platform': platform}
    rds.hmset(urlhash, redis_hmset_dict)


def crawl_batch_task(url_Lst):
    for url_info in url_Lst:
        crawl_a_url_and_update_redis(url_info['url'],
                                     url_info['platform'],
                                     url_info['urlhash'])


def scan_redis_to_crawl():
    batch_size = 1000
    cur = 0
    task_batchs = []
    scan_counter = 0
    while True:
        scan_counter += 1
        if scan_counter%5 == 0:
            print(scan_counter, 'cur:', cur, datetime.datetime.now())
        cur, hash_keys = rds.scan(cur)
        for urlhash in hash_keys:
            if len(urlhash) == 40:
                url_d = rds.hgetall(urlhash)
                url = url_d[b'url'].decode()
                platform = url_d[b'platform'].decode()
                is_fetched = int(url_d[b'is_fetched'].decode())
                if is_fetched == 0:
                    task_batchs.append({'url': url,
                                        'platform': platform,
                                        'urlhash': urlhash})
                    if len(task_batchs) == batch_size:
                        # multi-processing here
                        crawl_batch_task(task_batchs)
                        task_batchs.clear()
        if cur == 0:
            break



def remove_fetched_url_from_redis(remove_interval=10):
    time.sleep(remove_interval)
    cur = 0
    delete_counter = 0
    while True:
        cur, hash_keys = rds.scan(cur)
        for urlhash in hash_keys:
            if len(urlhash) == 40:
                url_d = rds.hgetall(urlhash)
                try:
                    is_fetched = int(url_d[b'is_fetched'].decode())
                    if is_fetched == 1:
                        rds.delete(urlhash)
                        delete_counter += 1
                except:
                    pass
        if cur == 0:
            break
    print('delete_counter', delete_counter)
    return delete_counter
