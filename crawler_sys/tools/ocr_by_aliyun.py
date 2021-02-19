# -*- coding:utf-8 -*-
# @Time : 2020/5/29 15:52 
# @Author : litao


import os,re
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.acs_exception.exceptions import ClientException
from aliyunsdkcore.acs_exception.exceptions import ServerException
from aliyunsdkocr.request.v20191230.RecognizeCharacterRequest import RecognizeCharacterRequest
from crawler.crawler_sys.framework.config.oss_keyword import AccessKeyId,AccessKeySecret
import oss2
from viapi.fileutils import FileUtils


endpoint = "oss-cn-beijing.aliyuncs.com"
# region = "v-plus-scope.oss-cn-beijing.aliyuncs.com"
region = "v-plus-scope"
oss_url_expries = 3600
access_key_id = AccessKeyId
access_key_secret = AccessKeySecret
bucket_name = "v-plus-scope"
auth = oss2.Auth(access_key_id, access_key_secret)
bucket = oss2.Bucket(auth, endpoint, bucket_name)
client = AcsClient(AccessKeyId, AccessKeySecret, 'cn-shanghai')


def put_obj(access_key_id, access_key_secret, region, bucket_name, object_name):
    fileobj_content = open("ocr_img/")
    # oss2.set_file_logger(log_file_path, 'oss2', logging.ERROR)
    auth = oss2.Auth(access_key_id, access_key_secret)
    bucket = oss2.Bucket(auth, region, bucket_name)
    bucket.put_object(object_name, fileobj_content)
    file_utils = FileUtils(AccessKeyId, AccessKeySecret)
    oss_url = file_utils.get_oss_url("http://xxx.jpeg", "jpg", False)

def ocr_from_aliyun(file_name=""):
    request = RecognizeCharacterRequest()
    request.set_accept_format('json')
    path_name = "ocr_img/%s"%file_name
    put_obj(AccessKeyId, AccessKeySecret, endpoint, region, path_name,)
    request.set_ImageURL("http://explorer-image.oss-cn-shanghai.aliyuncs.com/270450672578492833/2020-05-01+200210.png?OSSAccessKeyId=LTAI4Fk9FstqSEYnqKJ5Dpeo&Expires=1590740750&Signature=ZggX6U2%2F3WvpSUpR9P8EYrD0vbQ%3D")
    request.set_MinHeight(15)
    request.set_OutputProbability(True)

    response = client.do_action_with_exception(request)
    # python2:  print(response)
    print(str(response, encoding='utf-8'))


def file_path_scan(file_path):
    for filename in os.listdir(file_path):
        path = os.path.join(file_path, filename)
        if not os.path.isfile(path):
            continue
        title = img_to_str(path, lang=Languages.CHS)
        print(title)
        try:
            play_count = re.findall("\d+",title)[0]
            #print(play_count)
        except:
            #print(title)
            play_count= 0
        yield filename,play_count


file_path = r'D:\work_file\word_file_new\litao\num'
for filename,play_count in file_path_scan(file_path):
    time_str = filename.replace(".png","")
    time_str = time_str[0:13] +":"+ time_str[13:15]+":"+ time_str[15:]
    # print(time_str)
    print(time_str,play_count)