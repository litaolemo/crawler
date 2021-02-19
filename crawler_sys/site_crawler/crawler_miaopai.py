# -*- coding: utf-8 -*-
"""
Created on Fri Sep 28 10:28:45 2018

@author: fangyucheng
"""


import urllib
import requests
import json
from bs4 import BeautifulSoup
from crawler_sys.utils.output_results import retry_get_url
from crawler_sys.utils.trans_str_play_count_to_int import trans_play_count
from crawler.crawler_sys.utils.util_logging import logged
try:
    from .func_get_releaser_id import *
except:
    from func_get_releaser_id import *

class Crawler_miaopai():

    def get_releaser_follower_num(self, releaserUrl):
        if "www.yixia.com" in releaserUrl:
            get_page = retry_get_url(releaserUrl)
            get_page.encoding = 'utf-8'
            page = get_page.text
            soup = BeautifulSoup(page, 'html.parser')
            try:
                midstep_1 = soup.find('ul', {'class': 'bottomInfor'})
                midstep_2 = midstep_1.find_all('li')
                for line in midstep_2:
                    line_text = line.text
                    if '粉丝' in line_text:
                        follower_str = line_text.replace('粉丝', '')
                        follower_num = trans_play_count(follower_str)
                print('%s follower number is %s' % (releaserUrl, follower_num))
                return follower_num
            except:
                print("can't can followers")
        elif "n.miaopai.com" in releaserUrl:
            try:
                split_url = releaserUrl.split("personal/")
                suid = split_url[-1].replace('.htm', '').replace('.html', '').replace('htm', '')
                url = "https://n.miaopai.com/api/aj_user/space.json?suid=%s" % suid
                get_page = urllib.request.urlopen(url)
                page_bytes = get_page.read()
                page_str = page_bytes.decode("utf-8")
                page_dic = json.loads(page_str)
                follower_num = page_dic['data']['followers_count']
                return follower_num
            except:
                print("can't can followers")


    def get_releaser_id(self, releaserUrl):
        return get_releaser_id(platform=self.platform, releaserUrl=releaserUrl)


#encoding method
    @logged
    def releaser_page(self, releaserUrl, releaser_page_num_max=30):
        headers = {'Host': 'n.miaopai.com',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                   'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                   'Accept-Encoding': 'gzip, deflate, br',
                   'Connection': 'keep-alive',
                   'Cookie': 'aliyungf_tc=AQAAAIVvfVl0CgQAysVBfBViNUJYGG5C; Hm_lvt_e8fa5926bca558076246d7fb7ca12071=1545124849; Hm_lpvt_e8fa5926bca558076246d7fb7ca12071=1545124849',
                   'Upgrade-Insecure-Requests': '1',
                   'Cache-Control': 'max-age=0'}
        releaser_id = self.get_releaser_id(releaserUrl)
        page_num = 1
        while page_num <= releaser_page_num_max:
            url = ('https://n.miaopai.com/api/aj_user/medias.json?suid=%s&page=%s'
                   % (releaser_id, page_num))
            get_page = requests.get(url, headers=headers)
            get_page.encoding = 'utf-8'
            page = get_page.text
            page_dic = get_page.json()

if __name__ == "__main__":
    releaserUrl = 'http://n.miaopai.com/personal/h~NjA~vSfoYLz1pchtm'
    test = Crawler_miaopai()
    p = test.get_releaser_follower_num(releaserUrl)
