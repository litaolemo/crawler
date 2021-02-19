# -*- coding: utf-8 -*-
"""
Created on Wed May 23 12:37:48 2018

@author: fangyucheng
"""

import requests
# import js2py
import hashlib


def as_cp(user_id, max_behot_time):
    as_cp = []
    t = js2py.eval_js('var t = Math.floor((new Date).getTime() / 1e3)')
    i = js2py.eval_js('var t = Math.floor((new Date).getTime() / 1e3),i = t.toString(16).toUpperCase()')
    e = hashlib.md5(str(t).encode('utf-8')).hexdigest()
    if len(i) != 8:
        var_as = "479BB4B7254C150"
        cp = "7E0AC8874BB0985"
    else:
        e = e.upper()
        s = e[0:5]
        o = e[-5:]

        n = ''
        a = 0
        while a < 5:
            n = n + s[a] + i[a]
            a += 1

        l = ''
        r = 0
        while r < 5:
            l = l + i[r+3] + o[r]
            r = r + 1
    var_as = 'A1' + n + i[-3:]
    cp = i[0:3] + l + 'E1'

    as_cp.append(var_as)
    as_cp.append(cp)
    return as_cp


def signature(user_id, max_behot_time):
    jsurl = 'https://s3.pstatp.com/toutiao/resource/ntoutiao_web/page/profile/index_f62209a.js'
    get_page = requests.get(jsurl)
    get_page.encoding = 'utf-8'
    page = get_page.text
    effect_js = page.split('Function')
    js_1 = ('var navigator = {"userAgent":"Mozilla/5.0 (Windows NT 6.1; WOW64) '
             'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36"};')
    js_2 = 'Function' + effect_js[3]
    js_3 = 'Function' + effect_js[4]
    js_4 = ';function result(){ return TAC.sign('+user_id+''+max_behot_time+');} result();'
    js_total = js_1+js_2+js_3+js_4
    signature = js2py.eval_js(js_total)
    return signature
