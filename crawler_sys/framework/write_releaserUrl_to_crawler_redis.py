# -*- coding:utf-8 -*-
# @Time : 2019/9/11 11:48
# @Author : litao
import redis
import json
import datetime
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


def write_project_to_redis(project):
    rds.rpush("project", project)


def write_releaserUrl_to_redis(project, data,email_dic):
    project_name, duration = project.split("/")
    data_dict_for_redis = {"duration": duration, "data": json.dumps(data),"email":json.dumps(email_dic)}
    rds.hmset(project, data_dict_for_redis)
    write_project_to_redis(project)


def read_csv_write_into_redis(project_name, csv_file, crawler_days,email_dic=None):
    try:
        crawler_lis = []
        with open(csv_file, 'r', encoding="gb18030")as f:
            header_Lst = f.readline().strip().split(',')
            for line in f:
                line_Lst = line.strip().split(',')
                line_dict = dict(zip(header_Lst, line_Lst))
                platform = line_dict['platform']
                releaserUrl = line_dict['releaserUrl']
                crawler_lis.append(platform+"&"+releaserUrl)
        write_releaserUrl_to_redis("{0}/{1}".format(project_name, str(crawler_days)), crawler_lis,email_dic=email_dic)
        return True
    except:
        return False


def down_task():
    res = rds.hgetall("task_down")
    if res:
        rds.delete("task_down")
        return res
    else:
        return None


if __name__ == "__main__":
    # 传入的email_dic 格式如下
    mapping_dic = {
            "taskname": "127869453",
            "file_path": None,
            "data_str": None,
            "email_group": ["litao@csm.com.cn"],
            "email_msg_body_str": "任务已完成",
            "title_str": "任务已完成",
            "cc_group": [],
            "sender": "litao@csm.com.cn"
    }
    crawler_lis = []
    file = r'D:\work_file\发布者账号\一次性需求附件\大客户总台垂类及主持人增加账号.csv'
    read_csv_write_into_redis("task15", file, "1577808000000",email_dic=mapping_dic)
    print(down_task())
    # with open(file, 'r')as f:
    #     header_Lst = f.readline().strip().split(',')
    #     for line in f:
    #         line_Lst = line.strip().split(',')
    #         line_dict = dict(zip(header_Lst,line_Lst))
    #         platform = line_dict['platform']
    #         releaserUrl = line_dict['releaserUrl']
    #         crawler_lis.append(platform+"_"+releaserUrl)
    # write_releaserUrl_to_redis("project_name/1564588800000/3",crawler_lis)
