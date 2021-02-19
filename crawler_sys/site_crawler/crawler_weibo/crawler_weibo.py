# -*- coding: utf-8 -*-
"""
Created on Tue Aug 14 20:13:21 2018

@author: fangyucheng
"""

import re
import rsa
import time
import json
import urllib
import base64
import binascii
import requests

from bs4 import BeautifulSoup


class Crawler_weibo():

    def __init__(self, timeout=None, platform='weibo'):

        self.session = requests.Session()
        self.headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Encoding': 'gzip, deflate, sdch',
                        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
                        'Connection': 'keep-alive',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2128.59 Safari/537.36'}

    def manipulate_login(self, user_name, password):
        # cookie写入cookie_pool
        cookie_pool = open('cookie_pool',
                           'a', encoding='utf-8')

        # 转换用户名
        user_name_quote = urllib.parse.quote_plus(user_name)
        user_name_base64 = base64.b64encode(user_name_quote.encode('utf-8'))
        user_name_b64 = user_name_base64.decode('utf-8')

        # 获得servertime pubkey rsakv nonce 四个参数
        current_time = int(time.time() * 1000)
        login_url_first_part = 'http://login.sina.com.cn/sso/prelogin.php?'
        login_url_dic = {'entry': 'weibo',
                         'callback': 'sinaSSOController.preloginCallBack',
                         'su': user_name_b64,
                         'rsakt': 'mod',
                         'checkpin': '1',
                         'client': 'ssologin.js(v1.4.18)',
                         '_': current_time}
        login_url_second_part = urllib.parse.urlencode(login_url_dic)
        login_url = login_url_first_part + login_url_second_part
        get_page = requests.get(login_url)
        get_page.encoding = 'utf-8'
        page = get_page.text
        page_rep = page.replace('sinaSSOController.preloginCallBack', '')
        page_dic = eval(page_rep)
        pubkey = page_dic['pubkey']
        servertime = page_dic['servertime']
        rsakv = page_dic['rsakv']
        nonce = page_dic['nonce']

        # 构造密码
        rsa_pubkey = int(pubkey, 16)
        key = rsa.PublicKey(rsa_pubkey, 65537)
        message = str(servertime) + '\t' + str(nonce) + '\n' + str(password)
        message = message.encode("utf-8")
        password_rsa = rsa.encrypt(message, key)
        password_bi = binascii.b2a_hex(password_rsa)

        # login，通过post，获得cookie
        post_url = 'http://login.sina.com.cn/sso/login.php?client=ssologin.js(v1.4.18)'
        post_data_dic = {'encoding': 'UTF-8',
                         'entry': 'weibo',
                         'from': '',
                         'gateway': '1',
                         'nonce': nonce,
                         'pagerefer': "",
                         'prelt': 67,
                         'pwencode': 'rsa2',
                         "returntype": "META",
                         'rsakv': rsakv,
                         'savestate': '7',
                         'servertime': servertime,
                         'service': 'miniblog',
                         'sp': password_bi,
                         'sr': '1920*1080',
                         'su': user_name_b64,
                         'useticket': '1',
                         'vsnf': '1',
                         'url': 'http://weibo.com/ajaxlogin.php?framelogin=1&callback=parent.sinaSSOController.feedBackUrlCallBack&display=0&'}

        logining_page = self.session.post(post_url, data=post_data_dic, headers=self.headers)
        login_loop = logining_page.content.decode("GBK")

        if '正在登录' in login_loop or 'Signing in' in login_loop:
            cookie = logining_page.cookies.get_dict()
            print(cookie,type(cookie))
            current_time = int(time.time() * 1000)
            cookie_dic = {'cookie': cookie,
                          'current_time': current_time}
            cookie_json = json.dump(cookie_dic,cookie_pool)
            print('got cookie in login process')
            return cookie
        else:
            print('post failed, suggest to login again')


    def test_cookie(self, test_url=None,
                    cookie=None,
                    user_name=None,
                    password=None):
        if test_url is None:
            test_url = 'https://weibo.com/1188203673/GuV3o9VYt'
        get_page = requests.get(test_url, cookies=cookie)
        page = get_page.text
        length = len(page)
        if length > 20000:
            print("due to the page's length is %s, cookie is useful" % length)
            return cookie
        else:
            print("invalid cookie at the page length %s" % length)
            return None

    def get_weibo_info_from_search_page(self, retrieve_soup, cookie):
        try:
            weibo_id = retrieve_soup.find('div', {'action-type': 'feed_list_item'})['mid']
            user_id_str = retrieve_soup.find('div', {'action-type': 'feed_list_item'})['tbinfo']
            user_id = re.findall('\d+', user_id_str)[0]
        except:
            try:
                weibo_id_str = retrieve_soup.find('a', {'action-type': 'fl_menu'})['action-data']
                weibo_id = re.findall('\d+', weibo_id_str)[0]
                user_id_str = retrieve_soup.find('a', {'class': 'name_txt'})['usercard']
                user_id = re.findall('\d+', ' '.join(re.findall('id=\d+', user_id_str)))[0]
            except:
                weibo_id = None
                user_id = None
                print('id_error')
        try:
            user_url = retrieve_soup.find('div', {'class': 'face'}).a['href']
        except:
            user_url = None
            print('user_url error')
        try:
            user_nickname = retrieve_soup.find('div', {'class': 'face'}).a['title']
        except:
            user_nickname = None
            print('user_nickname error')
        try:
            weibo_content = retrieve_soup.find('p', {'class': 'comment_txt'}).text
            weibo_content = weibo_content.strip('\n').strip()
            if '展开全文' in weibo_content:
                weibo_content = self.get_longtext(weibo_id, cookie)
        except:
            weibo_content = None
            print('weibo_content error')
        # 没有判断是否为超级话题 #没有摘取emoji #没有下载图片
        try:
            release_time = retrieve_soup.find('a', {'class': 'W_textb'})['date']
        except:
            try:
                release_time = retrieve_soup.find('a', {'node-type': 'feed_list_item_date'})['date']
            except:
                release_time = None
                print('release_time error')
        try:
            weibo_url = retrieve_soup.find('a', {'class': 'W_textb'})['href']
        except:
            weibo_url = None
            print('weibo_url error')
        try:
            come_from = retrieve_soup.find('a', {'rel': 'nofollow'}).text
        except:
            come_from = None
            print("can't find come_from")
        try:
            repost_count = retrieve_soup.find('a', {'action-type': 'feed_list_forward'}).em.text
        except:
            repost_count = 0
        try:
            comment_count = retrieve_soup.find('a', {'action-type': 'feed_list_comment'}).em.text
        except:
            comment_count = 0
        try:
            favorite_count = retrieve_soup.find('a', {'action-type': 'feed_list_like'}).em.text
        except:
            favorite_count = 0
        fetch_time = int(time.time() * 1000)
        weibo_info = {'weibo_id': weibo_id,
                      'user_id': user_id,
                      'user_url': user_url,
                      'user_nickname': user_nickname,
                      'weibo_content': weibo_content,
                      'release_time': release_time,
                      'weibo_url': weibo_url,
                      'come_from': come_from,
                      'repost_count': repost_count,
                      'comment_count': comment_count,
                      'favorite_count': favorite_count,
                      'fetch_time': fetch_time}
        return weibo_info

    def get_repost_info(self, retrieve_soup):
        try:
            weibo_id = retrieve_soup['mid']
        except:
            weibo_id = None
            print('weibo_id error')
        try:
            user_id_str = retrieve_soup.div.a['usercard']
            user_id = re.findall('\d+', user_id_str)[0]
        except:
            user_id = None
            print('user_id error')
        try:
            user_url = retrieve_soup.div.a['href']
        except:
            user_url = None
            print('user_url error')
        try:
            user_nickname = retrieve_soup.find('div', {'class': 'WB_text'}).a.text
        except:
            user_nickname = None
            print('user_nickname error')
        try:
            weibo_content = retrieve_soup.find('span', {'node-type': 'text'}).text
            weibo_content = weibo_content.strip('\n').strip()
        except:
            weibo_content = None
            print('weibo_content error')
        if weibo_content is not None and '//' in weibo_content:
            parent_lst = weibo_content.split('//')
            try:
                pattern = '@.*:'
                parent_weibo = re.findall(pattern, parent_lst[1])[0].replace(':', '').replace('@', '')
            except:
                pattern = '@.*：'
                parent_weibo = re.findall(pattern, parent_lst[1])[0].replace('：', '').replace('@', '')
        else:
            parent_weibo = None
        # 没有判断是否为超级话题 #没有摘取emoji #没有下载图片
        try:
            release_time = retrieve_soup.find('a', {'node-type': 'feed_list_item_date'})['date']
        except:
            release_time = None
            print('release_time error')
        try:
            weibo_url = retrieve_soup.find('a', {'node-type': 'feed_list_item_date'})['href']
        except:
            weibo_url = None
            print('weibo_url error')
        try:
            repost_count_str = retrieve_soup.find('a', {'action-type': 'feed_list_forward'}).text
            repost_count_lst = re.findall('\d+', repost_count_str)
            if repost_count_lst != []:
                repost_count = repost_count_lst[0]
            else:
                repost_count = 0
        except:
            repost_count = 0
        try:
            favorite_count_str = retrieve_soup.find('span', {'node-type': 'like_status'}).text
            favorite_count_str.replace('ñ', '')
            try:
                favorite_count_lst = re.findall('\d+', favorite_count_str)
                if favorite_count_lst != []:
                    favorite_count = favorite_count_lst[0]
                else:
                    favorite_count = 0
            except:
                favorite_count = 0
                print('favorite_count is zero')
        except:
            favorite_count = 0
        fetch_time = int(time.time() * 1000)
        repost_info = {'weibo_id': weibo_id,
                       'user_id': user_id,
                       'user_url': user_url,
                       'user_nickname': user_nickname,
                       'weibo_content': weibo_content,
                       'parent_weibo': parent_weibo,
                       'release_time': release_time,
                       'weibo_url': weibo_url,
                       'repost_count': repost_count,
                       'favorite_count': favorite_count,
                       'fetch_time': fetch_time}
        return repost_info

    def get_user_weibo_info(self, retrieve_soup, cookie):
        try:
            weibo_id = retrieve_soup['mid']
        except:
            weibo_id = None
            print('weibo_id error')
        try:
            user_nickname = retrieve_soup.find('a', {'class': 'W_f14'}).text
        except:
            user_nickname = None
            print('user_nickname error')
        try:
            weibo_content = retrieve_soup.find('div', {'class': 'WB_text'}).text
            weibo_content = weibo_content.strip('\n').strip()
            if '展开全文' in weibo_content:
                weibo_content = self.get_longtext(weibo_id, cookie)
        except:
            weibo_content = None
            print('weibo_content error')
        # 没有判断是否为超级话题 #没有摘取emoji #没有下载图片
        try:
            release_time = retrieve_soup.find('a', {'class': 'S_txt2'})['date']
        except:
            release_time = None
            print('release_time error')
        try:
            text = "weibo.com"
            weibo_url = text.join((retrieve_soup.find('a', {'class': 'S_txt2'})['href']))
            print(weibo_url)
        except:
            weibo_url = None
            print('weibo_url error')
        try:
            come_from = retrieve_soup.find('a', {'rel': 'nofollow'}).text
        except:
            come_from = None
            print("can't find come_from")
        try:
            repost_count_lst = retrieve_soup.find('span', {'node-type': 'forward_btn_text'}).find_all('em')
            for line in repost_count_lst:
                try:
                    repost_count = int(line.text)
                except:
                    repost_count = 0
        except:
            repost_count = 0
        try:
            comment_count_lst = retrieve_soup.find('span', {'node-type': 'comment_btn_text'}).find_all('em')
            for line in comment_count_lst:
                try:
                    comment_count = int(line.text)
                except:
                    comment_count = 0
        except:
            comment_count = 0
        try:
            favorite_count_lst = retrieve_soup.find('span', {'node-type': 'like_status'}).find_all('em')
            for line in favorite_count_lst:
                try:
                    favorite_count = int(line.text)
                except:
                    favorite_count = 0
        except:
            favorite_count = 0
            print('favorite_count is zero')
        fetch_time = int(time.time() * 1000)
        weibo_info = {'weibo_id': weibo_id,
                      'user_nickname': user_nickname,
                      'weibo_content': weibo_content,
                      'come_from': come_from,
                      'release_time': release_time,
                      'weibo_url': weibo_url,
                      'repost_count': repost_count,
                      'comment_count': comment_count,
                      'favorite_count': favorite_count,
                      'fetch_time': fetch_time}
        return weibo_info

    def get_longtext(self, weibo_id, cookie):
        current_time = int(time.time() * 1000)
        longtext_url = ('https://weibo.com/p/aj/mblog/getlongtext?ajwvr=6&mid='
                        + weibo_id + '&is_settop&is_sethot&is_setfanstop&'
                                     'is_setyoudao&__rnd=' + str(current_time))
        get_page = requests.get(longtext_url, headers=self.headers, cookies=cookie)
        try:
            page_dic = get_page.json()
            wait_for_soup = page_dic['data']['html']
            soup = BeautifulSoup(wait_for_soup, 'html.parser')
            longtext = soup.text
            return longtext
        except:
            print("can't get longtext")
            return ''

    def search_page(self, keyword, user_name, password):
        result_lst = []
        openfile = open('D:/python_code/crawler/crawler_sys/site_crawler/crawler_weibo/error5',
                        'a', encoding='utf-8')
        cookie = self.manipulate_login(user_name=user_name,
                                       password=password)
        # cookie = self.test_cookie(get_cookie)
        if cookie is not None:
            for page_num in range(1, 2):
                search_url = 'http://s.weibo.com/weibo/' + keyword + '?b=1&page=' + str(page_num)
                get_page = requests.get(search_url, headers=self.headers, cookies=cookie)
                get_page.encoding = 'utf-8'
                page = get_page.text
                print(len(page))
                time.sleep(10)
                soup = BeautifulSoup(page, 'html.parser')
                sfa = soup.find_all('script')
                find_content = ''
                for line in sfa:
                    if '"pid":"pl_weibo_direct"' in str(line):
                        find_content = str(line)
                if find_content != '':
                    find_content1 = find_content.replace('<script>STK && STK.pageletM && STK.pageletM.view(', '')
                    find_content2 = find_content1.replace(')</script>', '')
                    find_content_dic = json.loads(find_content2)
                    content = find_content_dic['html']
                    content_soup = BeautifulSoup(content, 'html.parser')
                    weibo_lst = content_soup.find_all('div', {'class': 'WB_cardwrap'})
                    for line in weibo_lst:
                        try:
                            weibo_info = self.get_weibo_info_from_search_page(line, cookie)
                            print('get_one_weibo_info')
                            result_lst.append(weibo_info)
                        except:
                            openfile.write(str(line))
                            openfile.write('\n')
                            openfile.flush()
                            print('error')
            return result_lst
        else:
            print('no valid cookie')
            return ''
        openfile.close()

    def repost_page(self, weibo_id, user_name, password):
        total_page = 0
        result_lst = []
        cookie = self.manipulate_login(user_name=user_name,
                                       password=password)
        # cookie = self.test_cookie(get_cookie)
        if cookie is not None:
            current_time = int(time.time() * 1000)
            repost_url = 'https://weibo.com/aj/v6/mblog/info/big?ajwvr=6&id=' + weibo_id + '&max_id=0&page=1&__rnd=' + str(
                current_time)
            get_page = requests.get(repost_url, headers=self.headers, cookies=cookie)
            get_page.encoding = 'utf-8'
            try:
                page_dic = get_page.json()
                total_page = page_dic['data']['page']['totalpage']
                repost_info = page_dic['data']['html']
                repost_soup = BeautifulSoup(repost_info, 'html.parser')
                repost_agg = repost_soup.find_all('div', {'action-type': 'feed_list_item'})
                for line in repost_agg:
                    try:
                        one_repost = self.get_repost_info(line)
                        result_lst.append(one_repost)
                        print('get one repost')
                    except:
                        print('one repost data error')
                print(one_repost)
            except:
                print("can't get repost data")
        time.sleep(6)
        if cookie is not None and total_page != 0:
            for page_num in range(1, total_page + 1):
                current_time = int(time.time() * 1000)
                repost_url = ('https://weibo.com/aj/v6/mblog/info/big?ajwvr=6&id=' + weibo_id +
                              '&max_id=0&page=' + str(page_num) + '&__rnd=' + str(current_time))
                get_page = requests.get(repost_url, headers=self.headers, cookies=cookie)
                time.sleep(3)
                get_page.encoding = 'utf-8'
                try:
                    page_dic = get_page.json()
                    total_page = page_dic['data']['page']['totalpage']
                    repost_info = page_dic['data']['html']
                    repost_soup = BeautifulSoup(repost_info, 'html.parser')
                    repost_agg = repost_soup.find_all('div', {'action-type': 'feed_list_item'})
                    for line in repost_agg:
                        one_repost = self.get_repost_info(line)
                        result_lst.append(one_repost)
                        print('get one repost at %s' % page_num)
                    print(one_repost)
                except:
                    print("can't get repost data")
        if result_lst != []:
            return result_lst
        else:
            print("can't get repost data")
            return None

    def user_page(self, user_id, user_name, password):
        result_lst = []
        cookie_pool = open('cookie_pool',
                           'r', encoding='utf-8')
        for coo in cookie_pool:
            print(coo)
            cookie = json.loads(coo)
        #cookie = self.manipulate_login(user_name=user_name,password=password)
        #cookie = {"ALC": "ac%3D2%26bt%3D1561705868%26cv%3D5.0%26et%3D1593241868%26ic%3D-621306587%26login_time%3D1561705868%26scf%3D%26uid%3D7211103954%26vf%3D0%26vs%3D0%26vt%3D0%26es%3Db91c9d11ca009f8c4f48080505ae615b", "LT": "1561705868", "tgc": "TGT-NzIxMTEwMzk1NA==-1561705868-tc-6005B5FEAADCEB07A63BA0D6D544CF92-1", "ALF": "1593241868", "SCF": "Ah7YtXJ_s6ue4BJWekcj8HMaZEYi3Kel5243tYoDHC9y0TD9y7MYKIhYu7fV0_BEaPmgGpFKmkyz-WA-cF6-Vgc.", "SUB": "_2A25wEc3cDeRhGeFM6lMQ8C3FzjiIHXVTZrgUrDV_PUNbm9AKLULSkW9NQP7JKShhH9bCX9VIpjzhPXX89XiDiHbj", "SUBP": "0033WrSXqPxfM725Ws9jqgMF55529P9D9WFmSG3DWrqckklXmwYD.UNJ5NHD95QNeo2peK501K-XWs4DqcjMi--NiK.Xi-2Ri--ciKnRi-zNeKM7eKM0SheX15tt", "sso_info": "v02m6alo5qztKWRk5yljpOQpZCToKWRk5iljoOgpZCjnLGNs4CxjbOMtIyzkLiJp5WpmYO0t4yjhLGMk4CzjpOUtA==", "login": "609423641c81693ee710ee69b0d0e34c"}
        if cookie is not None:
            for page_num in range(1, 3):
                first_url = ('https://weibo.com/u/' + user_id + '?visible=0&is_all=1&is_tag=0'
                                                                '&profile_ftype=1&page=' + str(page_num) + '#feedtop')
                get_page = requests.get(first_url, headers=self.headers, cookies=cookie)
                get_page.encoding = 'utf-8'
                page = get_page.text
                soup = BeautifulSoup(page, 'html.parser')
                sfa = soup.find_all('script')
                find_content = ''
                for line in sfa:
                    if 'Pl_Official_MyProfileFeed__' in str(line):
                        find_content = str(line)
                find_content = find_content.replace('<script>FM.view(', '').replace(')</script>', '')
                # print(find_content)
                find_content_dic = json.loads(find_content)
                content_for_soup = find_content_dic['html']
                soup_content = BeautifulSoup(content_for_soup, 'html.parser')
                weibo_lst = soup_content.find_all('div', {'action-type': 'feed_list_item'})
                # time.sleep(15)
                for line_count,line in enumerate(weibo_lst):
                    weibo_info = self.get_user_weibo_info(line, cookie)
                    weibo_info['user_id'] = user_id
                    weibo_info['user_url'] = 'https://weibo.com/' + user_id
                    result_lst.append(weibo_info)
                    print('get data at element page:%s pagebar:%s' % (page_num,line_count))
                get_parameter = soup.find_all('script', {'type': 'text/javascript'})
                for line in get_parameter:
                    if 'pid' in str(line) and 'oid' in str(line):
                        parameter_str = str(line)
                parameter_str = parameter_str.replace('\r', '').replace('\n', '').replace("\'", '')
                domain = re.findall('\d+', ''.join(re.findall("pid]=\d+", parameter_str)))[0]
                special_id = re.findall('\d+', ''.join(re.findall("page_id]=\d+", parameter_str)))[0]
                current_time = int(time.time() * 1000)
                for pagebar in [0, 1]:
                    user_url = ('https://weibo.com/p/aj/v6/mblog/mbloglist?ajwvr=6&domain='
                                + domain + '&profile_ftype=1&is_all=1&pagebar=' + str(pagebar) +
                                '&pl_name=Pl_Official_MyProfileFeed__22&id=' + special_id +
                                '&script_uri=/' + user_id + '&feed_type=0&page=' + str(page_num) + '&pre_page=1'
                                                                                                   '&domain_op=' + domain + '&__rnd=' + str(
                                current_time))
                    get_page = requests.get(user_url, headers=self.headers, cookies=cookie)
                    get_page.encoding = 'utf-8'
                    try:
                        page_dic = get_page.json()
                        user_weibo_str = page_dic['data']
                        user_weibo_soup = BeautifulSoup(user_weibo_str, 'html.parser')
                        user_weibo_agg = user_weibo_soup.find_all('div', {'action-type': 'feed_list_item'})
                        # time.sleep(15)
                        for line in user_weibo_agg:
                            try:
                                weibo_info = self.get_user_weibo_info(line, cookie)
                                weibo_info['user_id'] = user_id
                                weibo_info['user_url'] = 'https://weibo.com/' + user_id
                                result_lst.append(weibo_info)
                                print('get data at ajax page page_num:%s pagebar:%s'
                                      % (page_num, pagebar))
                            except:
                                print('one weibo_info error')
                    except:
                        print('page error at page_num:%s pagebar:%s' % (page_num, pagebar))
        if result_lst != []:
            return result_lst
        else:
            print("can't get repost data")
            return None


if __name__ == '__main__':
    weibo = Crawler_weibo()
    user_name = '7255925880'
    password = 'Lemo1995'
    #    keyword = '罗奕佳'
    #    user_id = 'jianchuan'
    #    weibo_id = '4273575663592672'
    user_id = '1788283193'
    #    test_search2 = weibo.search_page(keyword, user_name, password)
    #    test_repost = weibo.repost_page(weibo_id, user_name, password)
    user_page = weibo.user_page(user_id, user_name, password)
    print(user_page)
