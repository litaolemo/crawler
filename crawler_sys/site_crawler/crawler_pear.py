# -*- coding: utf-8 -*-
"""
Created on Sun Apr  8 14:53:43 2018

@author: fangyucheng
"""

import datetime
import copy
import re
import json
from crawler_sys.framework.video_fields_std import Std_fields_video
from crawler_sys.utils.output_results import retry_get_url
from crawler_sys.utils.output_results import output_result
from crawler.crawler_sys.utils.util_logging import logged


class Crawler_pear():

    def __init__(self, timeout=None, platform='pearvideo'):
        if timeout == None:
            self.timeout = 10
        else:
            self.timeout = timeout
        self.platform = platform
        std_fields = Std_fields_video()
        self.video_data = std_fields.video_data
        self.video_data['platform'] = self.platform
        # remove fields that crawled data don't have
        pop_key_Lst = ['channel', 'describe', 'repost_count', 'isOriginal',
                       'video_id']
        for popk in pop_key_Lst:
            self.video_data.pop(popk)
        self.legal_list_page_urls = []
        self.legal_channels = []
        Cookie = ('JSESSIONID=E546B64FEB7F2009C3D2D64887F4FD67,'
                  'PEAR_UUID=7693819d-8037-460d-9a04-15217b3ee67f; '
                  'PEAR_DEVICE_FLAG=true;Hm_lvt_9707bc8d5f6bba210e7218b8496f076a=1522723194;'
                  'UM_distinctid=1600b8f08667c5-0c3c0cc44f7afd8-173a7640-1fa400-1600b8f08677c7; '
                  'PV_APP=srv-pv-prod-portal1; __ads_session=Ai82CnAsFAn2IyZjGwA=')
        self.headers = {'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Encoding':'gzip, deflate',
                        'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                        'Cache-Control':'max-age=0, no-cache',
                        'Connection':'keep-alive',
                        # Cookie is necessary to get results
                        'Cookie':Cookie,
                        'Host': 'app.pearvideo.com',
                        'Pragma':'no-cache',
                        'Upgrade-Insecure-Requests':'1',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:59.0) Gecko/20100101 Firefox/59.0'}
        self.category_corresponding = {'社会':'1', '世界':'2', '财富':'3', '娱乐':'4', '生活':'5',
                                       '美食':'6', '搞笑':'7', '科技':'8', '体育':'9', '知新':'10',
                                       '二次元':'17', '汽车':'31', '音乐':'59'}



    def video_page(self, url):
        find_video_id = ' '.join(re.findall('video_\d+', url))
        video_id = ' '.join(re.findall('\d+', find_video_id))
        real_url = 'http://app.pearvideo.com/clt/jsp/v3/content.jsp?contId='+video_id
        get_page = retry_get_url(real_url, headers=self.headers)
        get_page.encoding = 'utf-8'
        page = get_page.text
        page = page.replace('\n', '')
        page = page.replace('\r', '')
        page_dic = json.loads(page)
        if page_dic['resultMsg'] == 'success':
            self.video_data['title'] = page_dic['content']['name']
            self.video_data['video_id'] = video_id
            self.video_data['url'] = url
            self.video_data['releaser'] = page_dic['content']['authors'][0]['nickname']
            releaser_id = page_dic['content']['authors'][0]['userId']
            self.video_data['releaser_id'] = releaser_id
            self.video_data['releaserUrl'] = 'http://www.pearvideo.com/author_'+releaser_id
            self.video_data['comment_count'] = page_dic['content']['commentTimes']
            self.video_data['favorite_count'] = page_dic['content']['praiseTimes']
            rt_time = page_dic['content']['pubTime']
            self.video_data['release_time'] = int(datetime.datetime.strptime(rt_time,
                                                                             '%Y-%m-%d %H:%M').timestamp()*1e3)
            dura = page_dic['content']['duration']
            dura = dura.replace('"', '')
            dura_lst = dura.split("'")
            self.video_data['duration'] = (int(dura_lst[0]))*60+int(dura_lst[1])
            self.video_data['fetch_time'] = int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
            video_info = copy.deepcopy(self.video_data)
        else:
            print('error'+url)
        return video_info


    def video_page_by_video_id(self, video_id):
        real_url = 'http://app.pearvideo.com/clt/jsp/v3/content.jsp?contId='+video_id
        get_page = retry_get_url(real_url, headers=self.headers)
        get_page.encoding = 'utf-8'
        page = get_page.text
        page = page.replace('\n', '')
        page = page.replace('\r', '')
        page_dic = json.loads(page)
        if page_dic['resultMsg'] == 'success':
            self.video_data['title'] = page_dic['content']['name']
            self.video_data['video_id'] = video_id
            url = 'http://www.pearvideo.com/video_'+video_id
            self.video_data['url'] = url
            self.video_data['releaser'] = page_dic['content']['authors'][0]['nickname']
            releaser_id = page_dic['content']['authors'][0]['userId']
            self.video_data['releaser_id'] = releaser_id
            self.video_data['releaserUrl'] = 'http://www.pearvideo.com/author_'+releaser_id
            self.video_data['comment_count'] = page_dic['content']['commentTimes']
            self.video_data['favorite_count'] = page_dic['content']['praiseTimes']
            rt_time = page_dic['content']['pubTime']
            self.video_data['release_time'] = int(datetime.datetime.strptime(rt_time,
                                                                             '%Y-%m-%d %H:%M').timestamp()*1e3)
            dura = page_dic['content']['duration']
            dura = dura.replace('"', '')
            dura_lst = dura.split("'")
            self.video_data['duration'] = (int(dura_lst[0]))*60+int(dura_lst[1])
            self.video_data['fetch_time'] = int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
            video_info = copy.deepcopy(self.video_data)
        else:
            print('error'+url)
        return video_info


    @logged
    def releaser_page(self, releaserUrl,
                      error_file=None,
                      output_to_file=False, filepath=None,
                      output_to_es_raw=False,
                      output_to_es_register=False,
                      push_to_redis=False,
                      releaser_page_num_max=30):

        result_Lst = []
        find_releaser_id = ' '.join(re.findall('author_\d+', releaserUrl))
        releaser_id = ' '.join(re.findall('\d+', find_releaser_id))
        real_releaserUrl = ('http://app.pearvideo.com/clt/jsp/v3/userHome.jsp?userId='
                            +releaser_id+'&reqType=1&start=0')
        count = 0

        while real_releaserUrl != '' and count < releaser_page_num_max*10:
            count += 10
            get_page = retry_get_url(real_releaserUrl, headers=self.headers)
            get_page.encoding='utf-8'
            page = get_page.text
            print('get page done')
            page = page.replace('\n', '')
            page = page.replace('\r', '')
            page_dic = json.loads(page)
            video_lst = page_dic['dataList']
            real_releaserUrl = page_dic['nextUrl']
            print('get next url:'+real_releaserUrl)
            for line in video_lst:
                try:
                    video_id = line['contInfo']['contId']
                    self.video_data['title'] = line['contInfo']['name']
                    self.video_data['video_id'] = video_id
                    self.video_data['url'] = 'http://www.pearvideo.com/video_'+video_id
                    self.video_data['releaser_id'] = releaser_id
                    self.video_data['releaserUrl'] = releaserUrl
                    self.video_data['favorite_count'] = line['contInfo']['praiseTimes']
                    rt_time =  line['pubTime']
                    self.video_data['release_time'] = int(datetime.datetime.strptime(rt_time, '%Y-%m-%d %H:%M').timestamp()*1e3)
                    dura = line['contInfo']['duration']
                    dura = dura.replace('"', '')
                    dura_lst = dura.split("'")
                    self.video_data['duration'] = (int(dura_lst[0]))*60+int(dura_lst[1])
                    self.video_data['fetch_time'] = int(datetime.datetime.timestamp(datetime.datetime.now())*1e3)
                    video_info = copy.deepcopy(self.video_data)
                    print('get one video_info')
                    result_Lst.append(video_info)

                    if len(result_Lst) >= 100:
                        output_result(result_Lst, self.platform,
                                      output_to_file=output_to_file,
                                      filepath=filepath,
                                      output_to_es_raw=output_to_es_raw,
                                      output_to_es_register=output_to_es_register,
                                      push_to_redis=push_to_redis,
                                     )
                        result_Lst.clear()
                except:
                    pass

        if len(result_Lst) != []:
            output_result(result_Lst, self.platform,
                          output_to_file=output_to_file,
                          filepath=filepath,
                          output_to_es_raw=output_to_es_raw,
                          output_to_es_register=output_to_es_register,
                          push_to_redis=push_to_redis,
                         )
            result_Lst.clear()


    def list_page(self, list_name,
                  error_file=None,
                  output_to_file=False,
                  filepath=None,
                  output_to_es_raw=False,
                  output_to_es_register=False,
                  push_to_redis=False,
                  total_wanted_video_num=100):
        category_id = self.category_corresponding[list_name]
        result_Lst = []
        count = 0
        list_url = 'http://app.pearvideo.com/clt/jsp/v3/getCategoryConts.jsp?categoryId='+category_id+'&start=0'
        while list_url != '' and count < total_wanted_video_num:
            get_page = retry_get_url(list_url, headers=self.headers)
            get_page.encoding = 'utf-8'
            page = get_page.text
            print('get page done')
            page = page.replace('\n', '')
            page = page.replace('\r', '')
            page_dic = json.loads(page)
            list_url = page_dic['nextUrl']
            video_lst_hot = page_dic['rankList']
            video_lst_gen = page_dic['contList']
            if video_lst_hot != []:
                for line in video_lst_hot:
                    try:
                        contid = line['contId']
                        line_dic = self.video_page_by_video_id(contid)
                        print('get one line done')
                        result_Lst.append(line_dic)
                    except:
                        pass
            for line in video_lst_gen:
                try:
                    contid = line['contId']
                    line_dic = self.video_page_by_video_id(contid)
                    print('get one line done')
                    result_Lst.append(line_dic)
                except:
                    pass

            count += len(video_lst_gen)+len(video_lst_hot)
            print(count)

            if len(result_Lst) >= 100:
                output_result(result_Lst, self.platform,
                              output_to_file=output_to_file,
                              filepath=filepath,
                              output_to_es_raw=output_to_es_raw,
                              output_to_es_register=output_to_es_register,
                              push_to_redis=push_to_redis,
                             )
                result_Lst.clear()

        if len(result_Lst) != []:
            output_result(result_Lst, self.platform,
                          output_to_file=output_to_file,
                          filepath=filepath,
                          output_to_es_raw=output_to_es_raw,
                          output_to_es_register=output_to_es_register,
                          push_to_redis=push_to_redis,
                         )
            result_Lst.clear()


    def search_page(self, keyword=None, search_pages_max=30,
                    output_to_es_raw=False,
                    output_to_es_register=False,
                    es_index=None,
                    doc_type=None):
        list_page = []
        count = 0
        pages = 0
        while pages < search_pages_max:
            pages += 1
            try:
                url = ('http://app.pearvideo.com/clt/jsp/v3/search.jsp?k='
                       + keyword
                       + '&start='
                       + str(count))
                get_page = retry_get_url(url, headers=self.headers)
                get_page.encoding = 'utf-8'
                page = get_page.text
                print('get page done')
                page = page.replace('\n', '')
                page = page.replace('\r', '')
                page_dic = json.loads(page)
                midstep = page_dic['searchList']
                count += 10
                for line in midstep:
                    try:
                        contid = line['contId']
                        line_dic = self.video_page_by_video_id(contid)
                        print('get one line done')
                        list_page.append(line_dic)
                        if len(list_page) >= 100:
                            output_result(result_Lst=list_page,
                                          platform=self.platform,
                                          output_to_es_raw=output_to_es_raw,
                                          output_to_es_register=output_to_es_register,
                                          es_index=es_index,
                                          doc_type=doc_type)
                            list_page.clear()
                    except:
                        pass
            except:
                pass
        if list_page != []:
            output_result(result_Lst=list_page,
                          platform=self.platform,
                          output_to_es_raw=output_to_es_raw,
                          output_to_es_register=output_to_es_register,
                          es_index=es_index,
                          doc_type=doc_type)

        return list_page

if __name__=='__main__':
    pear = Crawler_pear()
#     #videopage = pear.video_page(url='http://www.pearvideo.com/video_1400210')
# #    releaserUrl = 'http://www.pearvideo.com/author_11406235'
# #    pear.releaser_page(releaserUrl,
# #                       error_file=None,
# #                       output_to_file=False, filepath=None,
# #                       output_to_es_raw=False,
# #                       output_to_es_register=False,
# #                       push_to_redis=False,
# #                       releaser_page_num_max=30)
#     #releaser2=pear.releaser_page(userid='10006693')
#     listpage = pear.list_page(list_name='社会',
#                   error_file=None,
#                   output_to_file=False,
#                   filepath=None,
#                   output_to_es_raw=True,
#                   output_to_es_register='test2',
#                   push_to_redis='test_pear',
#                   total_wanted_video_num=30)
    sr_pearv = pear.search_page(keyword='任正非 BBC', search_pages_max=4)
