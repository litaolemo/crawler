# -*- coding:utf-8 -*-
# @Time : 2019/11/1 15:55 
# @Author : litao
import requests
import datetime
from elasticsearch.helpers import scan
from elasticsearch import Elasticsearch
import redis,time
import logging
import oss2
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor
# import torch.multiprocessing
# torch.multiprocessing.set_sharing_strategy('file_system')

try:
    from write_data_into_es.func_cal_doc_id import *
except:
    from write_data_into_es_new.func_cal_doc_id import *

hosts = '192.168.17.11'
port = 80
user = 'litao'
passwd = 'lQSmSEnGZZxl'
http_auth = (user, passwd)
es = Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)
pool = redis.ConnectionPool(host='192.168.17.60', port=6379, db=8)
rds = redis.Redis(connection_pool=pool)


def get_short_video_image_hot(AccessKeyId, AccessKeySecret, endpoint, region):
    platform_list = ["抖音","toutiao","new_tudou","kwai","腾讯视频","腾讯新闻","haokan","网易新闻"]
    for platform in platform_list:
        search_body = {
        "query": {
            "bool": {
                    "filter": [
                            {
                                    "term": {
                                            "platform.keyword": platform
                                    }
                            },
                            {"range": {"release_time": {"gte": int((datetime.datetime.now() + datetime.timedelta(days=-7)).timestamp()*1e3)}}},
                            #{"range": {"play_count": {"gte": 1000}}}
                    ], "must": [{"exists": {"field": "video_img"}}]
            }
    }}
        scan_res = scan(es,query=search_body,index="ronghe_recommend_staging,ronghe_recommend",raise_on_error=False,scroll='50m',
                        request_timeout=300)
        # scan_res = scan(es, query=search_body,
        #                 index="short-video-all-time-url",
        #                 doc_type="all-time-url",
        #                 scroll='50m',
        #                 raise_on_error=False,
        #                 request_timeout=300)
        for res in scan_res:
            _id = cal_doc_id(platform=res["_source"]["platform"], url=res["_source"]["url"], doc_id_type="all-time-url",
                             data_dict=res["_source"])
            image_url = res["_source"].get("video_img")
            if not image_url:
                continue
            date_str = datetime.datetime.fromtimestamp(res["_source"]["release_time"] / 1e3).strftime("%Y/%m/%d")
            path_name = "media/data/video-title-images/%s/%s/%s/%s.jpg" % (
            res["_source"]["platform"], res["_source"]["releaser_id_str"], date_str, _id)
            put_obj(AccessKeyId, AccessKeySecret, endpoint, region, path_name, image_url, _id)


def get_short_video_image(AccessKeyId, AccessKeySecret, endpoint, region):
    platform_list = ["抖音","toutiao","new_tudou","kwai","腾讯视频","腾讯新闻","haokan","网易新闻"]
    for platform in platform_list:
        search_body = {
        "query": {
            "bool": {
                    "filter": [
                            {
                                    "term": {
                                            "platform.keyword": platform
                                    }
                            },
                            {"range": {"release_time": {"gte": int((datetime.datetime.now() + datetime.timedelta(days=-7)).timestamp()*1e3)}}},
                            #{"range": {"play_count": {"gte": 1000}}}
                    ], "must": [{"exists": {"field": "video_img"}}]
            }
    }}
        scan_res = scan(es,query=search_body,index="short-video-production", doc_type="daily-url",  raise_on_error=False,scroll='50m',
                        request_timeout=300)
        # scan_res = scan(es, query=search_body,
        #                 index="short-video-all-time-url",
        #                 doc_type="all-time-url",
        #                 scroll='50m',
        #                 raise_on_error=False,
        #                 request_timeout=300)
        for res in scan_res:
            _id = cal_doc_id(platform=res["_source"]["platform"], url=res["_source"]["url"], doc_id_type="all-time-url",
                             data_dict=res["_source"])
            image_url = res["_source"].get("video_img")
            if not image_url:
                continue
            date_str = datetime.datetime.fromtimestamp(res["_source"]["release_time"] / 1e3).strftime("%Y/%m/%d")
            path_name = "media/data/video-title-images/%s/%s/%s/%s.jpg" % (
            res["_source"]["platform"], res["_source"]["releaser_id_str"], date_str, _id)
            put_obj(AccessKeyId, AccessKeySecret, endpoint, region, path_name, image_url, _id)


def get_weibo_image(AccessKeyId, AccessKeySecret, endpoint, region):
    search_body = {
            "query": {
                    "bool": {
                            "filter": [
                                    {"range": {"release_time": {"gte": int(
                                        (datetime.datetime.now() + datetime.timedelta(days=-7)).timestamp() * 1e3)}}}
                            ]
                             , "must": [{"exists": {"field": "wb_pic"}}]
                    }
            }}
    scan_res = scan(es, query=search_body, index="ronghe_weibo_monthly", doc_type="doc", raise_on_error=False,
                    scroll='50m',
                    request_timeout=300)
    for res in scan_res:
        _id = res["_source"]["wb_bowen_id"]
        image_url = res["_source"]["wb_pic"]
        if "," in image_url:
            image_url = image_url.split(",")[0]
        date_str = datetime.datetime.fromtimestamp(res["_source"]["release_time"] / 1e3).strftime("%Y/%m/%d")
        path_name = "media/data/weibo-title-images/%s/%s/%s/%s.jpg" % (
                res["_source"]["platform"], res["_source"]["UID"], date_str, _id)
        put_obj(AccessKeyId, AccessKeySecret, endpoint, region, path_name, image_url, _id)


def get_avatar_image(AccessKeyId, AccessKeySecret, endpoint, region):
    search_body = {
            "query": {
                    "bool": {
                            "filter": [
                                    {"range": {"fetch_time": {"gte": int(
                                            (datetime.datetime.now() + datetime.timedelta(
                                                days=-7)).timestamp() * 1e3)}}}
                            ]
                    }}
    }
    scan_res = scan(es, query=search_body, index="releaser_fans", doc_type="doc", raise_on_error=False,
                    scroll='50m',
                    request_timeout=300)
    for res in scan_res:
        platform = res["_source"]["platform"]
        try:
            if res["_source"]["platform"] == "weibo":
                image_url = res["_source"]["wb_touxiang_url"]
                _id = res["_source"]["UID"]
            else:
                image_url = res["_source"]["releaser_img"]
                _id = res["_source"]["releaser_id_str"]
        except:
            continue
        path_name = "media/data/releasers-avatar/%s/%s.jpg" % (
                platform, _id)
        put_obj(AccessKeyId, AccessKeySecret, endpoint, region, path_name, image_url, _id)
# access_key_id = settings.access_key_id
# access_key_secret = settings.access_key_secret
# bucket_name = settings.bucket_name
# endpoint = settings.endpoint
# sts_role_arn = settings.sts_role_arn
# region = settings.live_record_region
# url_expries = settings.oss_url_expries

# 阿里云主账号AccessKey拥有所有API的访问权限，风险很高。强烈建议您创建并使用RAM账号进行API访问或日常运维，请登录 https://ram.console.aliyun.com 创建RAM账号。
# auth = oss2.Auth(access_key_id, access_key_secret)
# Endpoint以杭州为例，其它Region请按实际情况填写。
# bucket = oss2.Bucket(auth, 'http://oss-cn-shagnhai.aliyuncs.com', bucket_name)

# 设置此签名URL在60秒内有效。
# print('= ' * 20)
# print(bucket.sign_url('GET', '***.txt',3600))
log_file_path = "oss2.log"


def get_obj_url(access_key_id, access_key_secret, region, name, bucket_name, expires=3600):
    oss2.set_file_logger(log_file_path, 'oss2', logging.ERROR)
    auth = oss2.Auth(access_key_id, access_key_secret)
    bucket = oss2.Bucket(auth, region, bucket_name)
    # object_meta = bucket.get_object_meta('object')
    return bucket.sign_url('GET', name, int(expires))


def put_obj(access_key_id, access_key_secret, region, bucket_name, object_name, fileId, _id):
    res = rds.set(_id, 1, ex=864000, nx=True)
    try:
        if res:
            reqObj = requests.get(fileId, timeout=3)
            fileobj_content = reqObj.content
            reqObj.close()
            # oss2.set_file_logger(log_file_path, 'oss2', logging.ERROR)
            auth = oss2.Auth(access_key_id, access_key_secret)
            bucket = oss2.Bucket(auth, region, bucket_name)
            bucket.put_object(object_name, fileobj_content)
            print("get %s img down"% _id)
        else:
            print("Already get image %s" % _id)
    except:
        rds.delete(_id)
    # object_meta = bucket.get_object_meta('object')


def put_obj_from_file(access_key_id, access_key_secret, region, bucket_name, local_file, target,
                      mime_type='image/jpeg'):
    oss2.set_file_logger(log_file_path, 'oss2', logging.ERROR)
    auth = oss2.Auth(access_key_id, access_key_secret)
    bucket = oss2.Bucket(auth, region, bucket_name)
    bucket.put_object_from_file(target, local_file)
    # object_meta = bucket.get_object_meta('object')


if __name__ == '__main__':
    from crawler.crawler_sys.framework.config.oss_keyword import AccessKeySecret,AccessKeyId
    endpoint = "oss-cn-beijing.aliyuncs.com"
    # region = "v-plus-scope.oss-cn-beijing.aliyuncs.com"
    region = "v-plus-scope"
    oss_url_expries = 3600
    access_key_id = AccessKeyId
    access_key_secret = AccessKeySecret
    bucket_name = "v-plus-scope"
    auth = oss2.Auth(access_key_id, access_key_secret)
    bucket = oss2.Bucket(auth, endpoint, bucket_name)
    # 上传一段字符串。Object名是motto.txt，内容是一段名言。
    # bucket.put_object(u'motto.txt', u'Never give up. - Jack Ma')
    # 下载到本地文件
    # bucket.get_object_to_file(u'motto.txt', u'localfile.txt')
    # 删除名为motto.txt的Object
    # bucket.delete_object(u'motto.txt')

    # # 清除本地文件
    # os.remove(u'localfile.txt')
    from multiprocessing import Process

    # p = Process(target=get_image,args=(AccessKeyId, AccessKeySecret, endpoint, region,))
    # p.start()
    # p.join()
    # print('主', p)
    # print('主线程/主进程')
    # print('* ' * 20)
    res_list = []
    executor = ProcessPoolExecutor(6)
    for task in range(5):
        # get_weibo_image(AccessKeyId, AccessKeySecret, endpoint, region)
        # get_avatar_image(AccessKeyId, AccessKeySecret, endpoint, region)
        res = executor.submit(get_short_video_image_hot,AccessKeyId, AccessKeySecret, endpoint, region)
        res_list.append(res)
    executor.shutdown(True)
    res_list = []
    executor = ProcessPoolExecutor(6)

    res_list = []
    executor = ProcessPoolExecutor(6)
    for task in range(5):
        # get_weibo_image(AccessKeyId, AccessKeySecret, endpoint, region)
        # get_avatar_image(AccessKeyId, AccessKeySecret, endpoint, region)
        res = executor.submit(get_short_video_image,AccessKeyId, AccessKeySecret, endpoint, region)
        res_list.append(res)
    executor.shutdown(True)
    res_list = []
    executor = ProcessPoolExecutor(6)
    for task in range(5):
        # get_weibo_image(AccessKeyId, AccessKeySecret, endpoint, region)
        # get_avatar_image(AccessKeyId, AccessKeySecret, endpoint, region)
        res = executor.submit(get_avatar_image,AccessKeyId, AccessKeySecret, endpoint, region)
        res_list.append(res)
    executor.shutdown(True)

    res_list = []
    executor = ProcessPoolExecutor(6)
    for task in range(5):
        # get_weibo_image(AccessKeyId, AccessKeySecret, endpoint, region)
        # get_avatar_image(AccessKeyId, AccessKeySecret, endpoint, region)
        res = executor.submit(get_weibo_image, AccessKeyId, AccessKeySecret, endpoint, region)
        res_list.append(res)
    executor.shutdown(True)
    # get_weibo_image(AccessKeyId, AccessKeySecret, endpoint, region)
    # get_avatar_image(AccessKeyId, AccessKeySecret, endpoint, region)
    # get_short_video_image(AccessKeyId, AccessKeySecret, endpoint, region)