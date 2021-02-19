# -*- coding:utf-8 -*-
# @Time : 2019/6/10 19:07
# @Author : litao
import time, datetime, random
import pandas as pd
import time, datetime
import urllib
from urllib import parse
try:
    from crawler_sys.site_crawler.func_get_releaser_id import *
except:
    from crawler.func_get_releaser_id import *


class Craler_qq(object):
    def get_releaser_id(self, releaserUrl):
        return get_releaser_id(platform="腾讯视频", releaserUrl=releaserUrl, is_qq=True)

    def get_release_time_from_str(self, rt_str):
        minute = '分钟'
        hour = '小时'
        day = '天'
        if minute in rt_str:
            rt_int = int(re.findall('\d+', rt_str)[0])
            rt = datetime.datetime.timestamp(datetime.datetime.now()) - rt_int * 60
            release_time = int(rt * 1e3)
        elif hour in rt_str:
            rt_int = int(re.findall('\d+', rt_str)[0])
            rt = datetime.datetime.timestamp(datetime.datetime.now()) - rt_int * 3600
            release_time = int(rt * 1e3)
        elif day in rt_str:
            rt_int = int(re.findall('\d+', rt_str)[0])
            rt = datetime.datetime.timestamp(datetime.datetime.now()) - rt_int * 3600 * 60
            release_time = int(rt * 1e3)
        else:
            release_time = int(datetime.datetime.strptime(rt_str, '%Y-%m-%d').timestamp() * 1e3)
        return release_time

    def retry_get_url(self,url, retrys=3, **kwargs):
        retry_c = 0
        while retry_c < retrys:
            try:
                get_resp = requests.get(url, **kwargs)
                return get_resp
            except:
                retry_c += 1
                time.sleep(1)
        print('Failed to get page %s after %d retries, %s'
              % (url, retrys, datetime.datetime.now()))
        return None
    # @logged
    def releaser_page(self, releaserUrl,
                      output_to_file=False, filepath=None,
                      output_to_es_raw=False,
                      output_to_es_register=False,
                      push_to_redis=False,
                      releaser_page_num_max=10000,
                      es_index=None,
                      doc_type=None):


        print('Processing releaserUrl %s' % releaserUrl)
        result_Lst = []
        releaser_info = self.get_releaser_id(releaserUrl)
        releaser_id = releaser_info['releaser_id']
        releaser = releaser_info['releaser']
        pagenum = 0
        if releaser_id != None:
            while pagenum <= releaser_page_num_max:
                pagenum += 1
                url_dic = {
                    "vappid": "50662744",
                    "vsecret": "64b037e091deae75d3840dbc5d565c58abe9ea733743bbaf",
                    "iSortType": "0",
                    "page_index": pagenum,
                    "hasMore": "true",
                    "stUserId": releaser_id,
                    "page_size": "20",
                    "_": datetime.datetime.now().timestamp()
                }
                releaser_page_url = ('http://access.video.qq.com/pc_client/GetUserVidListPage?%s' % urllib.parse.urlencode(url_dic))
                print('Page number: %d'% pagenum)
                try:
                    get_page = self.retry_get_url(releaser_page_url,
                                             timeout=3)
                except:
                    get_page = None
                if get_page != None and get_page.status_code == 200:
                    get_page.encoding = 'utf-8'
                    page = get_page.text
                    real_page = page[5:]
                    real_page = real_page.replace('null', 'None')
                    try:
                        get_page_dic = eval(real_page)
                        page_dic = get_page_dic["data"]['vecVidInfo']
                    except:
                        page_dic = None

                    if page_dic != None:
                        for a_video in page_dic:
                            try:
                                video_dic = {}
                                vid = a_video.get("vid")
                                video_info = a_video.get("mapKeyValue")
                                title = video_info['title']
                                play_count = int(float(video_info['view_all_count']))
                                rt_str = video_info['create_time']
                                release_time = datetime.datetime.strptime(rt_str,"%Y-%m-%d %H:%M")
                                url = "https://v.qq.com/x/page/%s.html" % vid
                                duration = int(video_info['duration'])
                                fetch_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1e3)
                                video_dic['releaserUrl'] = releaserUrl
                                video_dic['title'] = title
                                video_dic['duration'] = duration
                                video_dic['url'] = url
                                video_dic['play_count'] = play_count
                                video_dic['releaser'] = releaser
                                video_dic['releaser_id_str'] = releaser_id
                                video_dic['release_time'] = int(release_time.timestamp()*1e3)
                                video_dic['fetch_time'] = fetch_time
                                video_dic["video_id"] = vid
                                video_dic["releaser_id_str"] = "腾讯视频_%s"%(releaser_id)
                                yield video_dic
                            except Exception as e:
                                print(e)
                                continue


    def time_range_video_num(self, start_time, end_time, url_list):
        data_lis = []
        info_lis = []
        columns = ["title","url","release_time","releaserUrl","duration"]
        dur_count = 0
        count_false = 0
        for dic in url_list:
            for res in self.releaser_page(dic["url"]):
                title = res["title"]
                link = res["url"]
                video_time = res["release_time"]
                video_time_str = datetime.datetime.fromtimestamp(video_time / 1000).strftime("%Y-%m-%d %H-%M-%S")
                # print(res)

                if video_time:
                    if start_time < video_time:
                        if video_time < end_time:
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
        csv_save.to_csv("%s.csv" % (datetime.datetime.now().strftime("%Y-%m-%d")), encoding="gb18030", mode='a',
                        header=None, index=None)


if __name__ == "__main__":
    test = Craler_qq()
    url_lis = [
        {"platform": "腾讯视频",
         "url": "http://v.qq.com/vplus/f6969e939ac77ae83becd2356a3493c4/videos",
         "releaser": "芒果都市"
         },
    ]
    start_time = datetime.datetime(year=2019, month=6, day=6)
    end = datetime.datetime.now()
    start_time = 1556640000000
    end = int(end.timestamp()*1e3)
    test.time_range_video_num(start_time, end, url_lis)
