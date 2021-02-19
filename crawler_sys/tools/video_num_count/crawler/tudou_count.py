# -*- coding:utf-8 -*-
# @Time : 2019/6/10 19:07
# @Author : litao
import time, datetime, random
import pandas as pd
import os
import re
import copy
import time, datetime
import requests
import urllib
from bs4 import BeautifulSoup
from urllib import parse
try:
    from crawler_sys.utils.trans_strtime_to_timestamp import trans_strtime_to_timestamp
except:
    from crawler.trans_strtime_to_timestamp import trans_strtime_to_timestamp
try:
    from crawler_sys.site_crawler.func_get_releaser_id import *
except:
    from crawler.func_get_releaser_id import *
from crawler.trans_duration_str_to_second import trans_duration


class Craler_tudou(object):
    def get_releaser_id(self, releaserUrl):
        return get_releaser_id(platform="new_tudou", releaserUrl=releaserUrl)

    def get_releaser_name(self, releaserUrl):

        """
        Due to the function releaser_page can't get releaser name from api,
        I add a function to get it from web page
        posted by yucheng fang
        """

        get_page = requests.get(releaserUrl)
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        try:
            releaser = soup.find('div', {'class': 'user-name'}).a.text
        except:
            print("can't get releaser name at soup.find('div', {'class': 'user-name'}).a.text")
        if releaser is not None:
            print("get releaser name at soup.find('div', {'class': 'user-name'}).a.text")
            return releaser
        else:
            print("get releaser name at soup.find('div', {'class': 'user-name'}).a.text")
        try:
            releaser = soup.find('div', {'class': 'head-avatar'}).a['title']
        except:
            print("can't get releaser name at soup.find('div', {'class': 'head-avatar'}).a['title']")
        if releaser is not None:
            return releaser
        else:
            print("can't get releaser name at soup.find('div', {'class': 'head-avatar'}).a['title']")
        return None

    def rebuild_releaserUrl(self, releaserUrl):
        get_page = requests.get(releaserUrl)
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        releaser_id = soup.find('div', {'class': 'user-name'}).a['href']
        url = 'https://id.tudou.com' + releaser_id + '/videos'
        return url

    def video_page(self, url):
        video_info = {}
        get_page = requests.get(url)
        page = get_page.text
        soup = BeautifulSoup(page, 'html.parser')
        try:
            video_info['title'] = soup.find('h1', {'class': 'td-playbase__title'}).span.text
        except:
            video_info['title'] = None
        try:
            video_info['releaser'] = soup.find('a', {'class': 'td-play__userinfo__name'}).text
        except:
            video_info['releaser'] = None
        try:
            midsteptime = soup.find('div', {'class':
                                                'td-play__videoinfo__details-box__time'}).text[:-2]
            video_info['release_time'] = int(datetime.datetime.strptime(midsteptime,
                                                                        '%Y-%m-%d %H:%M:%S').timestamp() * 1e3)
        except:
            video_info['release_time'] = None
        try:
            video_info['releaserUrl'] = soup.find("a", {"class": "td-play__userinfo__name"})['href']
        except:
            video_info['releaserUrl'] = None
        try:
            find_play_count = ' '.join(re.findall('total_vv.*stripe_bottom', page))
            replace_comma_pcnt = find_play_count.replace(',', '')
            play_count_str = ' '.join(re.findall('total_vv":"\d+', replace_comma_pcnt))
            video_info['play_count'] = int(' '.join(re.findall('\d+', play_count_str)))
        except:
            video_info['play_count'] = 0
        try:
            find_comment_count = ' '.join(re.findall('total_comment.*recommend', page))
            replace_comma_ccnt = find_comment_count.replace(',', '')
            comment_count_str = ' '.join(re.findall('total_comment":"\d+', replace_comma_ccnt))
            video_info['comment_count'] = int(' '.join(re.findall('\d+', comment_count_str)))
        except:
            video_info['comment_count'] = 0
        try:
            find_dura = re.findall('stripe_bottom":"\d+:\d+', page)
            dura_str = ' '.join(find_dura).split('":"')[-1]
            video_info['duration'] = trans_duration(dura_str)
        except:
            video_info['duration'] = 0
        video_info['fetch_time'] = int(time.time() * 1e3)
        video_info['url'] = url
        print("get video data at %s" % url)
        return video_info

    def releaser_page(self, releaserUrl,
                      output_to_file=False,
                      filepath=None,
                      releaser_page_num_max=10000,
                      output_to_es_raw=False,
                      es_index=None,
                      doc_type=None,
                      output_to_es_register=False,
                      push_to_redis=False):

        """
        get video info from api instead of web page html
        the most scroll page is 1000
        """

        headers = {'Host': 'apis.tudou.com',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                   'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                   'Accept-Encoding': 'gzip, deflate',
                   'Connection': 'keep-alive',
                   'Cookie': ('isg=BIeH6gcJlwZw_xQESm9jlG-vFTuRJGXxikf0g1l0mJY9yKeKYVuAvzKJbkgzOzPm;'
                              'cna=XA2EFIGslWoCAWp4y3KXcZh7; ykss=cdbd115c102a68710215ad93;'
                              '__ysuid=1543316262167mjE; P_ck_ctl=62DE1D55DFE1C0F4F27A8662E6575F08;'
                              '__ayvstp=32'),
                   'Upgrade-Insecure-Requests': '1',
                   'Cache-Control': 'max-age=0'}

        count = 1
        #        has_more = True
        retry_time = 0
        result_list = []
        releaser_id = self.get_releaser_id(releaserUrl["url"])
        releaser = releaserUrl["releaser"]
        releaserUrl = 'https://id.tudou.com/i/%s/videos' % releaser_id
        url_dic = {"uid": releaser_id,
                   "pL": "20"}
        print("working on releaser: %s releaser_id: %s" % (releaser, releaser_id))
        while count <= releaser_page_num_max and retry_time < 8:
            url_dic['pg'] = str(count)
            url_dic['pn'] = str(count)
            api_url = 'http://apis.tudou.com/subscribe/v1/video?%s' % urllib.parse.urlencode(url_dic)
            print(api_url)
            get_page = requests.get(api_url, headers=headers)
            page_dic = get_page.json()
            #            has_more = page_dic.get('has_more')
            try:
                data_list = page_dic['entity']
            except:
                retry_time += 1
                time.sleep(0.3)
                print("no more data at releaser: %s page: %s try_time: %s" % (releaser, count, retry_time))
                continue
            if data_list == []:
                retry_time += 1
                time.sleep(0.3)
                print("no more data at releaser: %s page: %s try_time: %s" % (releaser, count, retry_time))
                continue
            else:
                retry_time = 0
                print("get data at releaser: %s page: %s" % (releaser, count))
                count += 1
                for info_dic in data_list:
                    video_info = {}
                    one_video = info_dic.get('detail')
                    if one_video is not None:
                        get_title = one_video.get('base_detail')
                        if get_title is not None:
                            video_info['title'] = get_title.get('title')
                        detail_info = one_video.get('video_detail')
                        if detail_info is not None:
                            video_id = detail_info.get('video_id')
                            if video_id is not None:
                                video_info['video_id'] = video_id
                                video_info['url'] = 'https://video.tudou.com/v/%s.html' % video_id
                            video_info['duration'] = detail_info.get('duration')
                            video_info['releaser_id_str'] = "new_tudou_%s" % (releaser_id)
                            video_info['comment_count'] = int(detail_info.get('comment_count'))
                            video_info['favorite_count'] = int(detail_info.get('praiseNumber'))
                            # favorite_count in database means 点赞数, while in web page the factor
                            # named praiseNumber
                            # in web page facorite_count means 收藏数
                            video_info['shoucang_count'] = (detail_info.get('favorite_count'))
                            # ('play_count', detail_info.get('vv_count'))
                            video_info['play_count'] = detail_info.get('vv_count')
                            release_time_str = detail_info.get('publish_time')
                            #                            print(video_info['release_time'])
                            if '天前' in release_time_str:
                                video_info['release_time'] = self.video_page(video_info['url'])['release_time']
                            else:
                                video_info['release_time'] = trans_strtime_to_timestamp(input_time=release_time_str,
                                                                                        missing_year=True)
                            video_info['fetch_time'] = int(time.time() * 1e3)
                            yield video_info

    def time_range_video_num(self, start_time, end_time, url_list):
        data_lis = []
        info_lis = []
        columns = ["title","url","release_time","releaserUrl","duration"]
        dur_count = 0
        count_false = 0
        for dic in url_list:
            for res in self.releaser_page(dic):
                title = res["title"]
                link = res["url"]
                video_time = res["release_time"]
                if video_time:
                    video_time_str = datetime.datetime.fromtimestamp(video_time / 1000).strftime("%Y-%m-%d %H-%M-%S")
                # print(res)

                if video_time:
                    if start_time <= video_time:
                        if video_time <= end_time:
                            data_lis.append((title, link, video_time_str, dic["url"],res["duration"]))
                            if int(res["duration"]) <= 600:
                                dur_count += 1
                    else:
                        count_false += 1
                        if count_false > 30:
                            break
                        else:
                            continue
            csv_save = pd.DataFrame(data_lis)
            if data_lis:
                try:
                    csv_save.to_csv("%s.csv" % (dic["platform"] + "_" + dic["releaser"]), encoding="gb18030",
                                    header=columns)
                except:
                    csv_save.to_csv("%s.csv" % (dic["platform"] + "_" + dic["releaser"]), encoding="utf-8",
                                    header=columns)
            info_lis.append([dic["platform"], dic["releaser"], len(data_lis),dur_count])
            data_lis = []
        csv_save = pd.DataFrame(info_lis)
        csv_save.to_csv("%s.csv" % (datetime.datetime.now().strftime("%Y-%m-%d")), encoding="gb18030",mode='a',header=None,index=None)


if __name__ == "__main__":
    test = Craler_tudou()
    url_lis = [
        {"platform": "new_tudou",
         "url": "https://id.tudou.com/i/UMjY0MDIyODg=/videos",
         "releaser": "湖北卫视"
         },
    ]
    start_time = datetime.datetime(year=2019, month=6, day=6)
    end = datetime.datetime.now()
    start_time = 1556640000000
    end = 1559318400000
    test.time_range_video_num(start_time, end, url_lis)
