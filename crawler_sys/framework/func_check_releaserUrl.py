# -*- coding:utf-8 -*-
# @Time : 2019/10/21 17:53 
# @Author : litao
from write_data_into_es.func_get_releaser_id import get_releaser_id as get_id
import redis, elasticsearch, time, datetime, sys
from crawler.crawler_sys.framework.update_data_in_redis_multi_process_auto_task import get_crawler
from write_data_into_es.target_releaser_add import write_to_es
from concurrent.futures import ThreadPoolExecutor
from maintenance.send_email_with_file_auto_task import write_email_task_to_redis
from selenium.webdriver.support.ui import WebDriverWait
from selenium import webdriver
import re, json
from qingboAPI.reback_data_api import add_weixin_releaser, add_releaser, del_releaser, get_account_scan, \
    get_biz_from_url
from elasticsearch.helpers import scan
from urllib import parse
import copy


from redis.sentinel import Sentinel

sentinel = Sentinel([('192.168.17.65', 26379),
                     ('192.168.17.66', 26379),
                     ('192.168.17.67', 26379)
                     ], socket_timeout=0.5)
# 查看master节点
master = sentinel.discover_master('ida_redis_master')
# 查看slave 节点
slave = sentinel.discover_slaves('ida_redis_master')
# 连接数据库
rds = sentinel.master_for('ida_redis_master', socket_timeout=0.5, db=2, decode_responses=True)


def check_releaserUrl(file):
    bulk_all_body = ""
    count = 0
    try:
        f = open(file, 'r', encoding="gb18030")
        head = f.readline()
        head_list = head.strip().split(',')
    except:
        f = file
    for i in f:
        if type(file) != list:
            line_list = i.strip().split(',')
            line_dict = dict(zip(head_list, line_list))
        else:
            line_dict = i
        # print(i)
        for k in line_dict:
            line_dict[k] = line_dict[k].strip().replace("\r", "").replace("\n", "").replace("\t", "")
        platform = line_dict['platform']
        releaser = line_dict['releaser']
        releaserUrl = line_dict['releaserUrl']
        releaser_id = get_id(platform=platform, releaserUrl=releaserUrl)
        if releaser_id:
            releaser_doc_id = platform + "_" + releaser_id
        else:
            releaser_doc_id = platform + "_" + releaser
        rds.lpush("releaser_doc_id_list", releaser_doc_id)


def carw_data_by_seleium(platfrom, releaserUrl, driver):
    if platfrom == "weibo":
        try:
            # driver.get(releaserUrl)
            # driver.save_screenshot("./screenshot.png")
            res = driver.find_element_by_xpath('//*[@id="Pl_Core_CustTab__2"]/div/div/table/tbody/tr/td[1]/a')
            url = res.get_attribute("href")
            releaser_id = get_id(platform="weibo", releaserUrl=url)
            print(url, releaser_id)
            if releaser_id:
                return releaser_id, url
            else:
                return None, None
        except:
            return None, None
    elif platfrom == "抖音":
        try:
            res = driver.page_source
            releaser_id = re.findall('uid: "(\d*)",', res, flags=re.DOTALL)[0]
            url = driver.current_url
            return "抖音_%s" % releaser_id, url
        except:
            return None, None
    elif platfrom == "miaopai":
        try:
            res = driver.find_element_by_xpath('//*[@id="app"]/div/header/div[1]/img')
            releaser_id = get_id(platform="miaopai", releaserUrl=releaserUrl)
            url = driver.current_url
            if releaser_id in url:
                return "miaopai_%s" % releaser_id, url
            else:
                return None, None
        except:
            return None, None
    return None, None


def delete_by_id(_id):
    data = ""
    data = data + ('{"delete": {"_id":"%s"}}\n' % _id)
    es.bulk(body=data, index="target_releasers", doc_type="doc")


def delete_wrong_id(_id):
    try:
        query = {
                "query": {
                        "bool": {
                                "filter": [
                                        {"term": {"releaser_id_str": _id.split("_", 1)[1]}}
                                ]
                        }
                }}
        res = es.search(index="department_purchase_log", doc_type="doc", body=query, timeout="2m")
        _id = res["hits"]["hits"][0]["_id"]
    except:
        return False
    data = ""
    data = data + ('{"delete": {"_id":"%s"}}\n' % _id)
    es.bulk(body=data, index="department_purchase_log", doc_type="doc")


def get_releaserUrl_from_es(releaser_id_str):
    global email_dic
    try:
        search_res = es.get("target_releasers", "doc", releaser_id_str)
    except:
        return None
    # print(search_res)
    res_data = search_res["_source"]
    _id = search_res["_id"]
    releaserUrl = res_data["releaserUrl"]
    releaser = res_data["releaser"]
    platform = res_data["platform"]
    post_by = res_data.get("post_by")
    is_purchased = res_data.get("is_purchased")
    print(platform, releaserUrl)
    crawler = get_crawler(platform)
    count_false = 0
    has_data = False
    if crawler:
        while count_false < 5:
            try:
                # 访问有效有数据
                crawler_instant = crawler()
                crawler_releaser_page = crawler_instant.releaser_page
                for single_data in crawler_releaser_page(releaserUrl=releaserUrl, proxies_num=8,
                                                         releaser_page_num_max=3):
                    if single_data["releaser_id_str"] == releaser_id_str:
                        res_data.update({"is_valid": "true", "has_data": 2})
                        # print(res_data)
                        res_data["releaser"] = single_data["releaser"]
                        write_to_es(data_list=[res_data], push_to_redis=False,put_to_es=True)
                        count_false = 5
                        has_data = True
                        break
                    else:
                        delete_by_id(releaser_id_str)
                        delete_wrong_id(releaser_id_str)
                        if post_by:
                            if not email_dic.get(post_by):
                                email_dic[post_by] = []
                            email_dic[post_by].append(
                                    releaser + " " + platform + " " + releaserUrl + " 错误,将替换为 %s \n" % single_data[
                                        "releaserUrl"])

                        res_data.update({"is_valid": "true", "has_data": 2, "releaserUrl": single_data["releaserUrl"]})
                        res_data["releaser"] = single_data["releaser"]
                        write_to_es(data_list=[res_data], push_to_redis=False,put_to_es=True)
                        # print(res_data)
                        has_data = True
                        count_false = 5
                        break
                if not has_data:
                    count_false = 5
                    raise Exception("has no data in", platform, releaserUrl)
            except Exception as e:
                # 访问有效无数据
                print(e)
                if count_false <= 5:
                    count_false += 1
                    continue
                res_data.update({"is_valid": "true", "has_data": 1})
                if post_by:
                    if not email_dic.get(post_by):
                        email_dic[post_by] = []
                    email_dic[post_by].append(
                            releaser + " " + platform + " " + releaserUrl + " 该releaserUrl 访问无数据\n")
                # print(res_data)
                write_to_es(data_list=[res_data], push_to_redis=False)
                count_false = 5
    else:
        # 供应商数据
        if res_data["platform"] in ["weixin"]:
            pass
        elif res_data["platform"] in ["抖音", "miaopai", "weibo",]:
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(releaserUrl)
            driver.implicitly_wait(5)
            check_id, check_url = carw_data_by_seleium(platform, releaserUrl, driver)
            driver.quit()
            if check_id:
                if check_id == releaser_id_str:
                    res_data.update({"is_valid": "true", "has_data": 2})
                    # print(res_data)
                    write_to_es(data_list=[res_data], push_to_redis=False,put_to_es=True)

                else:
                    delete_by_id(releaser_id_str)
                    if post_by:
                        if not email_dic.get(post_by):
                            email_dic[post_by] = []
                        email_dic[post_by].append(
                                releaser + " " + platform + " " + releaserUrl + " 错误,将替换为 %s \n" % check_url)
                    res_data.update({"is_valid": "true", "has_data": 2, "releaserUrl": check_url})
                    write_to_es(data_list=[res_data], push_to_redis=False,put_to_es=True)
                    # print(res_data)

            else:
                res_data.update({"is_valid": "false", "has_data": 0})
                if post_by:
                    if not email_dic.get(post_by):
                        email_dic[post_by] = []
                    email_dic[post_by].append(
                            releaser + " " + platform + " " + releaserUrl + " 该releaserUrl 访问无数据\n")
                # print(res_data)
                write_to_es(data_list=[res_data], push_to_redis=False,put_to_es=True)
        else:
            res_data.update({"is_valid": "flase", "has_data": 0})
            if post_by:
                if not email_dic.get(post_by):
                    email_dic[post_by] = []
                email_dic[post_by].append(
                        releaser + " " + platform + " " + releaserUrl + " 该平台数据暂无法解析 \n")
            write_to_es(data_list=[res_data], push_to_redis=False,put_to_es=True)
    if post_by and is_purchased:
        if not email_dic.get(post_by):
            email_dic[post_by] = []
        if platform == "weixin":
            biz = get_biz_from_url(res_data.get("releaserUrl"))
            if biz:
                search_body = {
                        "query": {
                                "bool": {
                                        "filter": [
                                                {"term": {"releaser_id_str.keyword": biz}},
                                                {"term": {"is_purchase": "true"}}
                                        ]
                                }
                        }
                }
                try:
                    if_exists = es.search(index="purchased_releasers", body=search_body)
                except Exception as e:
                    print(e, "can't find out wx_id")

                if if_exists["hits"]["total"] == 0:
                    try:
                        if "http" in res_data.get("releaserUrl"):
                            res_success, res_false = add_weixin_releaser(wx_url_list=[res_data.get("releaserUrl")])
                        else:
                            res_success, res_false = add_weixin_releaser(wx_biz_list=[biz])
                    except Exception as e:
                        print(e)
                        email_dic[post_by].append(
                                releaser + " " + platform + " " + releaserUrl + " 接口返回错误,请检查后重试 \n")
                    if res_success:
                        if res_success[0]:
                            res_data["releaser"] = res_success[0].get("wx_nickname")
                            res_data["releaser_id_str"] = res_success[0].get("wx_biz")
                            res_data["releaserUrl"] = res_success[0].get("wx_biz")
                            res_data["wx_id"] = res_success[0].get("wx_name")
                            if _id != "weixin_%s" % res_data["releaser_id_str"]:
                                delete_by_id(releaser_id_str)
                                delete_wrong_id(releaser_id_str)
                            res_data.update({"is_valid": "true", "has_data": 2, "is_purchased": 1})
                            update_dic = {
                                    "is_purchased": "true",
                                    "start_purchase_time": int(datetime.datetime.now().timestamp() * 1e3)
                            }
                            # print(res_data)
                            write_to_es(data_list=[res_data], push_to_redis=False, update_dic=update_dic,put_to_es=True)
                            email_dic[post_by].append(
                                    res_data["releaser"] + " " + platform + " " + res_data[
                                        "releaserUrl"] + " 已加入清博采购 \n")
                    if res_false:
                        if res_false[0]:
                            email_dic[post_by].append(
                                    releaser + " " + platform + " " + releaserUrl + " 加入清博接口失败,请检查后重试 提示%s \n" % str(
                                        res_false))
                            print("wx add error")
                            print(res_false)

                elif releaserUrl != biz:
                    res_data["releaser"] = if_exists["hits"]["hits"][0]["_source"].get("releaser")
                    res_data["releaser_id_str"] = if_exists["hits"]["hits"][0]["_source"].get("releaser_id_str")
                    res_data["releaserUrl"] = if_exists["hits"]["hits"][0]["_source"].get("releaser_id_str")
                    res_data["wx_id"] = if_exists["hits"]["hits"][0]["_source"].get("wx_id")
                    if _id != "weixin_%s" % res_data["releaser_id_str"]:
                        delete_by_id(releaser_id_str)
                    res_data.update({"is_valid": "true", "has_data": 2})
                    update_dic = {
                            "is_purchased": "true",
                            "start_purchase_time": int(datetime.datetime.now().timestamp() * 1e3)
                    }
                    # print(res_data)
                    write_to_es(data_list=[res_data], push_to_redis=False, update_dic=update_dic,put_to_es=True)
            else:
                email_dic[post_by].append(
                        releaser + " " + platform + " " + releaserUrl + " url无法访问,请检查后重试 \n")
        else:
            res = add_releaser(get_id(platform,releaserUrl),platform)
            if res:
                email_dic[post_by].append(
                        releaser + " " + platform + " " + releaserUrl + " 加入清博接口成功 \n")
            else:
                email_dic[post_by].append(
                        releaser + " " + platform + " " + releaserUrl + " 加入清博接口失败,请检查后重试 \n")


def craw_one_page_from_es():
    global email_dic
    now = datetime.datetime.now()
    while True and now.hour > 1:
        time.sleep(5)
        executor = ThreadPoolExecutor(max_workers=10)
        len_releaser_id_list = rds.llen("releaser_doc_id_list")
        while len_releaser_id_list > 0:
            releaser_id_str = rds.lpop("releaser_doc_id_list")
            len_releaser_id_list -= 1
            print(releaser_id_str)
            get_releaserUrl_from_es(releaser_id_str)
            # executor.submit(get_releaserUrl_from_es, releaser_id_str)
        executor.shutdown(wait=True)
        print(email_dic)
        for receiver in email_dic:
            email_msg_body_str = "问好:\n"
            for body in email_dic[receiver]:
                email_msg_body_str += body
            write_email_task_to_redis(
                    task_name="check_releaserUrl_%s" % str(int(datetime.datetime.now().timestamp() * 1e3)),
                    email_group=[receiver + "@csm.com.cn"],
                    sender=receiver + "@csm.com.cn", email_msg_body_str=email_msg_body_str, title_str="添加账号校验结果",
                    cc_group=["litao@csm.com.cn", "liushuangdan@csm.com.cn", "gengdi@csm.com.cn"])
        email_dic = {}
        print("timesleep 5")
        now = datetime.datetime.now()
    sys.exit(0)


def check_releaser():
    # 把清博的每日检测账号写入purchase_releaser中 并更新采购日期
    now = datetime.datetime.now()
    today_timestamp = int(datetime.datetime(year=now.year, month=now.month, day=now.day).timestamp() * 1e3)
    last_day_timestamp = int(today_timestamp - 86400000)

    def cheak_data(purchase_releaser_dic, add_dic, target_index, target_type, purchase_date_dic):
        bluk_dic = {}
        for count, res in enumerate(purchase_date_dic):
            if res in purchase_releaser_dic:
                continue
            else:
                res_body = copy.deepcopy(purchase_date_dic[res])
                bluk_body = {
                        "is_purchased":False,
                        "is_purchased_str":"未在清博接口中",
                        "platform":res.split("_", 1)[0],
                        "departments": {},
                        "releaser":res_body.get("releaser"),
                        "releaserUrl":res_body.get("releaserUrl"),
                }
                try:
                    res_body.pop("releaser", 0)
                    res_body.pop("releaserUrl", 0)
                except Exception as e:
                    print("data_dic_error", print(res_body))
                    continue
                for department in res_body:
                    if department != "purchase_history":
                        bluk_body["departments"][department] = res_body[department]["now"]
                    if res_body[department].get("purchase_history"):
                        if not res_body.get("purchase_history"):
                            bluk_body["purchase_history"] = {}
                        else:
                            bluk_body["purchase_history"][department] = bluk_body[department]["purchase_history"]
                bluk_dic[res] = bluk_body
            if count % 1000 == 0 and count != 1:
                bluk_data(bluk_dic, target_index=target_index, target_type=target_type)
                bluk_dic = {}
        bluk_data(bluk_dic, target_index=target_index, target_type=target_type)
        bluk_dic = {}
        for count, res in enumerate(purchase_releaser_dic):
            res_body = purchase_releaser_dic[res]
            res_body["is_purchased"] = False
            res_body["is_purchased_str"] = "停止采购"
            if add_dic.get(res):
                is_purchased_str = ""
                purchase_end_time_temp = 1
                res_body["is_purchased"] = True
                res_body["is_purchased_str"] = "采购中"

                if not res_body.get("start_purchase_time"):
                    res_body["start_purchase_time"] = last_day_timestamp
                res_body["end_purchase_time"] = today_timestamp
                if purchase_date_dic.get(res):
                    print(purchase_date_dic.get(res))
                    data_dic = copy.deepcopy(purchase_date_dic.get(res))
                    res_body["platform"] = res.split("_",1)[0]
                    res_body["releaser"] = data_dic.get("releaser")
                    try:
                        data_dic.pop("releaser",0)
                        data_dic.pop("releaserUrl",0)
                    except Exception as e:
                        print("data_dic_error",print(data_dic))
                        continue
                    if not res_body.get("departments"):
                        res_body["departments"] = {}
                    res_body["departments"] = {}
                    for department in data_dic:
                        if department != "purchase_history":
                            res_body["departments"][department] = data_dic[department]["now"]
                        if data_dic[department].get("purchase_history"):
                            if not res_body.get("purchase_history"):
                                res_body["purchase_history"] = {}
                            else:
                                res_body["purchase_history"][department] = data_dic[department]["purchase_history"]
                    for department in res_body["departments"]:
                        purchase_end_time = res_body["departments"][department]["purchase_end_time"]
                        if purchase_end_time >= purchase_end_time_temp:
                            purchase_end_time_temp = purchase_end_time
                    if purchase_end_time_temp == 7258089600000:
                        is_purchased_str = "长期采购"
                    else:
                        try:
                            is_purchased_str = datetime.datetime.fromtimestamp(purchase_end_time_temp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                        except:
                            is_purchased_str = "error"
                    res_body["is_purchased_str"] = is_purchased_str

                bluk_dic[res] = res_body
                if count % 1000 == 0 and count != 1:
                    bluk_data(bluk_dic, target_index=target_index, target_type=target_type)
                    bluk_dic = {}
        bluk_data(bluk_dic, target_index=target_index, target_type=target_type)

    def bluk_data(bluk_dic, target_index, target_type):
        bulk_all_body = ""
        for count, res in enumerate(bluk_dic):

            bulk_head = '{"index": {"_id":"%s"}}' % res
            data_str = json.dumps(bluk_dic[res], ensure_ascii=False)
            bulk_one_body = bulk_head + '\n' + data_str + '\n'
            bulk_all_body += bulk_one_body
            print("find", count)
            if count % 1000 == 0 and count != 1:
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

    def update_purchase_data_to_target_releasers(add_dict):
        count = 0
        bulk_all_body = ""
        search_body = {
                "query": {
                        "bool": {
                                "filter": [
                                        {"terms": {"platform.keyword": ["抖音", "腾讯新闻", "weixin", "miaopai", "toutiao",
                                                                        "kwai", "pearvideo"]}},
                                        # {"term": {"is_purchased": "true"}}
                                ]
                        }
                }
        }
        scan_res = scan(client=es, query=search_body, index="target_releasers")
        for res in scan_res:
            try:
                if add_dict.get(res["_id"]):
                    res["_source"]["is_purchased"] = True
                else:
                    res["_source"]["is_purchased"] = False
                bulk_head = '{"index": {"_id":"%s"}}' % res["_id"]
                data_str = json.dumps(res["_source"], ensure_ascii=False)
                bulk_one_body = bulk_head + '\n' + data_str + '\n'
                bulk_all_body += bulk_one_body
                count = count + 1
                if count % 500 == 0:
                    eror_dic = es.bulk(index='target_releasers', doc_type='doc',
                                       body=bulk_all_body)
                    bulk_all_body = ''
                    if eror_dic['errors'] is True:
                        print(eror_dic)
            except:
                continue
        if bulk_all_body != '':
            eror_dic = es.bulk(body=bulk_all_body,
                               index='target_releasers',
                               doc_type='doc',
                               )
            if eror_dic['errors'] is True:
                print(eror_dic)


    def delete_purchase_data(add_dict):
        releaser_id_str_dic = {}
        bulk_all_body = ""
        count = 0
        search_body = {
                "query": {
                        "bool": {
                                "filter": [
                                        {"range": {"end_purchase_time": {"lte": today_timestamp}}},
                                        {"term": {"is_purchased": "true"}}
                                ]
                        }
                }
        }

        # 处理所有过期账号,打标签is_purchase = 0
        scan_res = scan(client=es, query=search_body, index="department_purchase_log")
        for res in scan_res:
            # try:
            #     releaser_id = res["_source"]["releaser_id_str"] if res["_source"]["platform"] != "weixin" else "weixin_" + res["_source"]["releaser_id_str"]
            # except:
            #     continue
            # if releaser_id in add_dict:
            #     res["_source"]["is_purchased"] = True
            # else:
            res["_source"]["is_purchased"] = False
            if res["_source"].get("purchase_history"):
                res["_source"].get("purchase_history").append(
                        "%s-%s" % (res["_source"]["purchase_start_time"], res["_source"].get("purchase_end_time")))
            else:
                purchase_history = ["%s-%s" % (res["_source"]["purchase_start_time"], today_timestamp)]
                res["_source"]["purchase_history"] = purchase_history

            bulk_head = '{"index": {"_id":"%s"}}' % res["_id"]
            data_str = json.dumps(res["_source"], ensure_ascii=False)
            bulk_one_body = bulk_head + '\n' + data_str + '\n'
            bulk_all_body += bulk_one_body
            count = count + 1
            if count % 500 == 0:
                eror_dic = es.bulk(index='department_purchase_log', doc_type='doc',
                                   body=bulk_all_body)
                bulk_all_body = ''
                if eror_dic['errors'] is True:
                    print(eror_dic)
        if bulk_all_body != '':
            eror_dic = es.bulk(body=bulk_all_body,
                               index='department_purchase_log',
                               doc_type='doc',
                               )
            if eror_dic['errors'] is True:
                print(eror_dic)

        # 遍历所有采购账号,取出所有账号的采购时间
        purchase_date_dic = {}
        search_body = {
                "query": {
                        "bool": {
                                "filter": [
                                        {"terms": {"platform.keyword": ["抖音", "腾讯新闻", "weixin", "miaopai", "toutiao",
                                                                        "kwai", "pearvideo"]}},
                                        {"term": {"is_purchased": "true"}}
                                ], "must": [
                                        {"exists": {"field": "purchase_start_time"}}
                                ]
                        }
                }
        }
        scan_res = scan(client=es, query=search_body, index="department_purchase_log")
        for res in scan_res:
            try:
                if res["_source"]["platform"] == "weixin":
                    releaser_id = "weixin_" + res["_source"]["releaser_id_str"]
                elif res["_source"]["platform"] == "weibo":
                    releaser_id = "weibo_" + res["_source"]["releaser_id_str"]
                else:
                    releaser_id = res["_source"]["releaser_id_str"]
                try:
                    department = res["_source"]["department"]
                except:
                    department = res["_source"]["department_tags"]
                    if type(department) == list:
                        department = res["_source"]["department_tags"][0]


                releaser_id_str_dic[releaser_id] = res["_source"]
                if not purchase_date_dic.get(releaser_id):
                    purchase_date_dic[releaser_id] = {
                            department:{"now": {"purchase_start_time": res["_source"]["purchase_start_time"],
                                         "purchase_end_time": res["_source"].get("purchase_end_time") if res["_source"].get("purchase_end_time") else today_timestamp}},
                            "releaser":res["_source"].get("releaser"),
                            "releaserUrl":res["_source"].get("releaserUrl")
                    }
                else:
                    purchase_date_dic[releaser_id][department] = {
                            "now": {"purchase_start_time": res["_source"]["purchase_start_time"],"purchase_end_time": today_timestamp}
                    }
                if res["_source"].get("purchase_history"):
                    purchase_date_dic[releaser_id][department]["purchase_history"] = \
                        res["_source"]["purchase_history"]
                print(purchase_date_dic[releaser_id])

            except Exception as e:
                print(e)
                continue

        # 遍历清博账号,删除没有采购账号
        email_msg_body_str = ""
        for relaser_id_str in add_dict:
            if relaser_id_str not in releaser_id_str_dic:
                platform, releaser_id = relaser_id_str.split("_", 1)
                if platform != "weibo":
                    # res = del_releaser(platform=platform,releaser_id=releaser_id)
                    # releaser_id_str_dic[relaser_id_str]["is_purchase"] = Flase
                    # releaser_id_str_dic[relaser_id_str]["end_purchase_time"] = int(datetime.datetime.now().timestamp() *1e3)
                    email_msg_body_str += """     {0} {1} 在清博接口中,无部门采购 请检查\n""".format(platform, releaser_id)
                    print("del ", relaser_id_str)
            else:
                releaser_id_str_dic[relaser_id_str]["is_purchased"] = True
            res["_source"]["is_purchased"] = True
            bulk_head = '{"index": {"_id":"%s"}}' % res["_id"]
            data_str = json.dumps(res["_source"], ensure_ascii=False)
            bulk_one_body = bulk_head + '\n' + data_str + '\n'
            bulk_all_body += bulk_one_body
            count = count + 1
            if count % 500 == 0:
                eror_dic = es.bulk(index='department_purchase_log', doc_type='doc',
                                   body=bulk_all_body)
                bulk_all_body = ''
                if eror_dic['errors'] is True:
                    print(eror_dic)
        if bulk_all_body != '':
            eror_dic = es.bulk(body=bulk_all_body,
                               index='department_purchase_log',
                               doc_type='doc',
                               )
            if eror_dic['errors'] is True:
                print(eror_dic)

        write_email_task_to_redis(
                task_name="停止监测账号通知_%s" % str(int(datetime.datetime.now().timestamp() * 1e3)),
                email_group=["litao@csm.com.cn"],
                sender="litao@csm.com.cn", email_msg_body_str="问好:\n" + email_msg_body_str, title_str="停止监测账号通知",
                cc_group=["litao@csm.com.cn", "liushuangdan@csm.com.cn", "gengdi@csm.com.cn"])
        print(res)
        return purchase_date_dic

    def check_purchase_data():
        # 检查每日数据的采购账号 并打标签
        releaser_dic = {}
        add_dic = {}
        for count, a in enumerate(get_account_scan(platform="weixin")):
            releaser_dic["weixin_" + a["wx_biz"]] = a
        for count, a in enumerate(get_account_scan(platform="腾讯新闻")):
            releaser_dic["腾讯新闻_" + a["author_id"]] = a
        for count, a in enumerate(get_account_scan(platform="抖音")):
            releaser_dic["抖音_" + a["user_id"]] = a
        for count, a in enumerate(get_account_scan(platform="miaopai")):
            releaser_dic["miaopai_" + a["user_id"]] = a
        for count, a in enumerate(get_account_scan(platform="kwai")):
            releaser_dic["kwai_" + a["user_id"]] = a
        for count, a in enumerate(get_account_scan(platform="toutiao")):
            releaser_dic["toutiao_" + a["user_id"]] = a
        for count, a in enumerate(get_account_scan(platform="pearvideo")):
            releaser_dic["pearvideo_" + a["user_id"]] = a
        search_body = {
                "query": {
                        "bool": {
                                "filter": [
                                        {"term": {"data_month": now.month}},
                                        {"term": {"data_year": now.year}},
                                        {"term": {"platform.keyword": "weibo"}},
                                ]
                        }
                }
        }
        scan_res = scan(client=es, query=search_body, index="releaser_fans_monthly")
        for res in scan_res:
            releaser_dic["weibo_%s" % res["_source"]["UID"]] = res["_source"]

        purchase_releaser_dic = {}
        search_body = {
                "query": {
                        "bool": {
                                "filter": [
                                ]
                        }
                }
        }
        scan_res = scan(client=es, query=search_body, index="purchased_releasers")
        for res in scan_res:
            purchase_releaser_dic[res["_id"]] = res["_source"]

        for key in releaser_dic:
            if purchase_releaser_dic.get(key):
                res = releaser_dic[key]
            else:
                res = None
            platform = ""
            if not res:
                dic = {
                        "releaserUrl": releaser_dic[key].get("url"),
                        "releaser_id_str": key,
                        "timestamp": today_timestamp,
                        "is_purchased": True,
                        "start_purchase_time": today_timestamp,
                        "end_purchase_time": today_timestamp,
                        "platform": key.split("_",1)[0],
                }
                if "weixin" in key:
                    dic["wx_id"] = releaser_dic[key]["wx_name"]
                    dic["releaser_img"] = releaser_dic[key]["wx_logo"]
                    dic["releaser"] = releaser_dic[key]["wx_nickname"]
                    dic["releaser_id_str"] = releaser_dic[key]["wx_biz"]
                    dic["platform"] = "weixin"

                if "weibo" in key:
                    dic["releaser_id_str"] = releaser_dic[key]["UID"]
                    dic["UID"] = releaser_dic[key]["UID"]
                    dic["releaser"] = releaser_dic[key].get("releaser")
                    dic["releaserUrl"] = "https://weibo.com/u/%s" % releaser_dic[key].get("UID")
                    dic["platform"] = "weibo"
                add_dic[key] = dic

        update_purchase_data_to_target_releasers(releaser_dic)
        purchase_date_dic = delete_purchase_data(releaser_dic)

        # purchase_date_dic 为所有采购账号的采购时间
        bluk_data(add_dic, target_index="purchased_releasers", target_type="doc")
        cheak_data(add_dic=releaser_dic, target_index="purchased_releasers", target_type="doc",
                   purchase_releaser_dic=purchase_releaser_dic, purchase_date_dic=purchase_date_dic)

    check_purchase_data()


if __name__ == "__main__":
    # check_releaserUrl(r"D:\work_file\发布者账号\brief9月需求账号list (version 1).csv")
    email_dic = {}
    hosts = '192.168.17.11'
    port = 80
    user = 'ccr_managesys'
    passwd = 'Lu9i70pcV0Gc'
    http_auth = (user, passwd)
    es = elasticsearch.Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)
    now = datetime.datetime.now()
    if now.hour <= 9:
        try:
            check_releaser()
        except:
            pass
    craw_one_page_from_es()
    # do we have the crawler
    # """
    # 测试代码
    # """
    # get_releaserUrl_from_es("weixin_Mzg2MDA4Mzg0OQ==")
