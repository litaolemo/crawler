# -*- coding:utf-8 -*-
# @Time : 2019/9/12 10:19 
# @Author : litao

"""
    私密代理使用示例
    接口鉴权说明：
    目前支持的鉴权方式有 "simple" 和 "hmacsha1" 两种，默认使用 "simple"鉴权。
    所有方法均可添加关键字参数sign_type修改鉴权方式。
"""
import redis,random
import kdl,requests

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
rds = sentinel.master_for('ida_redis_master', socket_timeout=0.5, db=7, decode_responses=True)
# rds = redis.StrictRedis(host='192.168.17.60', port=6378, db=7, decode_responses=True)
def get_proxy_from_redis():
    try:
        one_proxy = rds.randomkey()
        username = "hanye"
        password = "i9mmu0a3"

        proxies = {
                "http": "http://%(user)s:%(pwd)s@%(ip)s/" % {'user': username, 'pwd': password, 'ip': one_proxy},
                "https": "http://%(user)s:%(pwd)s@%(ip)s/" % {'user': username, 'pwd': password, 'ip': one_proxy}
        }
        return proxies
    except Exception as e:
        print(e)
        return None

def func_get_proxy_to_redis():
    # chance = random.random()
    auth = kdl.Auth("997803479675913", "er2siw6i58c61s387sqljvovoz8zybaq")
    client = kdl.Client(auth)

    # 获取订单到期时间, 返回时间字符串
    # expire_time = client.get_order_expire_time()
    # print("expire time", expire_time)

    # 获取ip白名单, 返回ip列表
    # ip_whitelist = client.get_ip_whitelist()
    # print("ip whitelist", ip_whitelist)

    # 设置ip白名单，参数类型为字符串或列表或元组
    # 成功则返回True, 否则抛出异常
    # client.set_ip_whitelist([])
    # client.set_ip_whitelist("127.0.0.1, 192.168.0.139")
    # print(client.get_ip_whitelist())
    # client.set_ip_whitelist(tuple())

    # 提取私密代理ip, 第一个参数为提取的数量, 其他参数以关键字参数的形式传入(不需要传入signature和timestamp)
    # 具体有哪些参数请参考帮助中心: "https://help.kuaidaili.com/api/getdps/"
    # 返回ip列表
    # 注意：若您使用的是python2, 且在终端调用，或在文件中调用且没有加 "# -*- coding: utf-8 -*-" 的话
    # 传入area参数时，请传入unicode类型，如 area=u'北京,上海'
    # ips = client.get_dps(1, sign_type='simple', format='json', pt=2, area='北京,上海,广东')
    # print("dps proxy: ", ips)


    # 检测私密代理有效性： 返回 ip: true/false 组成的dict
    #ips = client.get_dps(1, sign_type='simple', format='json')
    # valids = client.check_dps_valid(ips)
    # print("valids: ", valids)

    # 获取私密代理剩余时间: 返回 ip: seconds(剩余秒数) 组成的dict
    ips = client.get_dps(1, format='json',dedup=1)
    seconds = client.get_dps_valid_time(ips)
    # print("seconds: ", seconds)
    for key in seconds:
        rds.set(key, key, ex=int(seconds[key]) - 3)

    # 获取计数版ip余额（仅私密代理计数版）
    # balance = client.get_ip_balance(sign_type='hmacsha1')
    # print("balance: ", balance)
def proxy_test(proxies):
    page_url = "http://dev.kdlapi.com/testproxy/"
    headers = {
            "Accept-Encoding": "Gzip",  # 使用gzip压缩传输数据让访问更快
    }

    res = requests.get(url=page_url, proxies=proxies, headers=headers)
    # print(res.status_code)  # 获取Reponse的返回码
    if res.status_code == 200:
        print(res.content.decode('utf-8'))  # 获取页面内容

def get_proxy_dic(max_proxies=None):
    if not max_proxies:
        max_proxies = 8
    try:
        res = rds.dbsize()
    except Exception as e:
        print("redis error")
        return None
    if res is None:
        return None
    if res < max_proxies:
        func_get_proxy_to_redis()
        return get_proxy_from_redis()
    else:
        return get_proxy_from_redis()

def get_proxy(proxies_num=None):
    if proxies_num:
        proxies = get_proxy_dic(max_proxies=proxies_num)
        # print("get a IP %s" % str(proxies))
        return proxies
    else:
        return None

if __name__ == "__main__":
    proxy_pool_dic = get_proxy(11)
    print(proxy_pool_dic)
    proxy_test(proxy_pool_dic)
    print(get_proxy_from_redis())