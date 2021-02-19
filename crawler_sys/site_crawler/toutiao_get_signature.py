# -*- coding:utf-8 -*-
# @Time : 2019/4/10 12:05 
# @Author : litao
import time
import hashlib
# import execjs

# def get_js(id):
#     # f = open(r"D:\work_file\test\index.js", 'r', encoding='UTF-8')
#     f = open(r"D:\work_file\work_file\crawler\crawler_sys\site_crawler\toutiao_test_js.js", 'r', encoding='UTF-8')
#     line = f.readline()
#     htmlstr = ''
#     while line:
#         htmlstr = htmlstr + line
#         line = f.readline()
#     ctx = execjs.compile(str(htmlstr))
#     return ctx.call('get_as_cp_signature',id)

def getHoney():  # 根据JS脚本破解as ,cp
    t = int(time.time())  # 获取当前时间
    # t=1534389637
    # print(t)
    e = str('%X' % t)  # 格式化时间
    # print(e)
    m1 = hashlib.md5()  # MD5加密
    m1.update(str(t).encode(encoding='utf-8'))  # 转化格式
    i = str(m1.hexdigest()).upper()  # 转化大写
    # print(i)
    n = i[0:5]  # 获取前5位字符
    a = i[-5:]  # 获取后5位字符
    s = ''
    r = ''
    for x in range(0, 5):  # 交叉组合字符
        s += n[x] + e[x]
        r += e[x + 3] + a[x]
    eas = 'A1' + s + e[-3:]
    ecp = e[0:3] + r + 'E1'
    # print(eas)
    # print(ecp)
    return eas, ecp


