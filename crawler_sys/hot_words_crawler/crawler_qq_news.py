# -*- coding:utf-8 -*-
# # @Time : 2020/3/4 10:33
# # @Author : litao



import requests
import json, re, datetime, urllib
from crawler.crawler_sys.utils.output_results import retry_get_url
from crawler.crawler_sys.utils.output_results import hot_words_output_result, output_result
from crawler.crawler_sys.utils.trans_strtime_to_timestamp import trans_strtime_to_timestamp
from crawler.crawler_sys.utils.trans_duration_str_to_second import trans_duration
from crawler.crawler_sys.utils.trans_str_play_count_to_int import trans_play_count
from write_data_into_es.func_cal_doc_id import *
import base64
from urllib.parse import parse_qs, urlparse



class Crawler_Qq_News(object):
    def __init__(self):
        self.platform = "腾讯新闻"
        self.headers = {
        "Cookie": "lskey=;skey=;uin=; luin=;logintype=0; main_login=;",
        "RecentUserOperation": "1_news_news_top,1__qqnews_custom_search,2_DailyHotDetailActivity",
        "Referer": "http://inews.qq.com/inews/android/",
        "User-Agent": "%E8%85%BE%E8%AE%AF%E6%96%B0%E9%97%BB6040(android)",
        "Host": "w.inews.qq.com",
        "Connection": "Keep-Alive",
        "Accept-Encoding": "gzip",
        "client-ip-v4": "114.250.251.100",


        }

    def get_hot_words(self):
        bulk_list = []
        timestamp = int(datetime.datetime.now().timestamp() * 1e3)
        url = "https://w.inews.qq.com/searchPage?pagefrom=moreHotDetail&adcode=310112&is_special_device=0&mid=0&dpi=320.0&qqnetwork=wifi&rom_type=R11-user%205.1.1%20NMF26X%20500200210%20release-keys&isColdLaunch=1&real_device_width=2.81&net_proxy=DIRECT@&net_bssid=48:A4:72:58:86:D5&isMainUserLogin=0&currentChannelId=_qqnews_custom_search&isElderMode=0&apptype=android&islite=0&hw=OPPO_OPPOR11&baseid=&global_session_id={0}&screen_width=900&omgbizid=&isClosePersonalized=0&sceneid=&videoAutoPlay=1&imsi=460077203886213&fix_store=&cpuabi=armeabi-v7a&isoem=0&currentTabId=news_news&lite_version=&startTimestamp={1}&net_slot=0&qn-time={2}&pagestartfrom=icon&mac=48:A4:72:58:86:D5&activefrom=icon&net_ssid=R1148a4725886d57203&store=17&screen_height=1600&top_activity=DailyHotDetailActivity&real_device_height=5.0&origin_imei=866174725888628&network_type=wifi&origCurrentTab=top&global_info=1|1|1|1|1|14|4|1|0|6|1|1|1||0|J309P000000000:J902P000000000:J601P900000000:A601P800217702:A601P700321102:B601P600286205:A601P500154501:A601P400161601:J601P300000000:B601P200096102:A601P100272502:A601P000261102:J601P904000000:J601P903000000:A601P902266601:A601P901291001:J601P811000000:A601P701226201:A601P622269601:A601P621294101:A601P620269601:J601P111000000:J601P110000000:A601P109107102:A601P105118803:A601P019237403:A601P016212405:J601P006000000:J603P000000000:J401P100000000:A401P000050901:J602P900000000:J602P800000000:J602P700000000:J602P600000000:A602P500267502:B602P400286004:J602P300000000:J602P200000000:J602P100000000:B602P000315504:A602P901257901:J602P616000000:A602P615304801:A602P613271701:A602P611253801:A602P516234601:A602P414259901:A602P307160708:J602P302000000:A602P208205801:J602P117000000:A602P007272801:A602P003136401:J304P000000000:J310P700000000:A310P200210802:J310P100000000:B310P020314103:A310P010301701:B310P000267107:B701P000323002:A703P000322204:A704P000309801:J702P000000000:J405P000000000:J064P400000000:J064P300000000:B064P100243802:B064P020290902:J064P010000000:J064P000000000:A085P000087701:B074P200238202:J074P040000000:B074P030315703:A074P020315602:A074P010315401:B074P000142402:J903P000000000:A267P300215801:A267P200263601:A267P100299801:B267P000300102:A073P040317201:B073P030314503:A073P020313801:J073P010000000:B073P000313603:J060P700000000:J060P300000000:J060P200000000:B060P100299703:A060P090287301:J060P020000000:J060P010000000:B060P000311102:J060P099000000:J060P016000000:A406P000313203:J403P700000000:J403P600000000:A403P200206702:B403P100246105:J403P010000000:A403P000310401:A403P602218702:B404P200262402:A404P000263407:J055P200000000:J055P090000000:J055P080000000:J055P070000000:J055P060000000:J055P050000000:J055P010000000:A055P000265801:J402P100000000:J402P090000000:J402P080000000:J402P060000000:J402P020000000:A402P000301403:J054P400000000:J054P300000000:J054P200000000:A054P100269701:B054P090289604:A054P080289702:J054P050000000:J054P040000000:A054P030288501:J054P010000000:A054P000319901:J056P000000000:A901P200252304:B901P100226405:B901P000232405:J407P000000000|1402|0|1|25|25|0|0|0||3|3|1|1|1|1|1|1|-1|0|0|5|2|0|0|0|3|0|0|1|3|0|2|0|0|2|0|0|1|0|1|1|0|0|1|0|4|0|1|1|11|20|1|0|1|1|0|0|1|4|0|1|1|41|2|51|60|0|1|0|0|1|5|1|0|0|71|0|0|1|71&imsi_history=460077203886213&net_apn=0&uid=48a4725886d57203&omgid=&trueVersion=6.0.40&qimei=866174725888628&devid=866174725888628&appver=22_android_6.0.40&Cookie=lskey%3D;skey%3D;uin%3D;%20luin%3D;logintype%3D0;%20main_login%3D;%20&qn-sig=74f9daefb0544a8cff5f2d1e0b465fd0&qn-rid=1002_1218eb69-f164-474c-9330-04d620a35c93&qn-newsig=68896f7d5f840c8540a9dff8877c88c277879f095408ca81edffe1a8b05ba94f".format(timestamp,int(timestamp/1000),timestamp)
        page_res = retry_get_url(url, headers=self.headers, proxies=3, timeout=10)
        page_json = page_res.json()
        for data in page_json["showInfo"]:
            search_title = data["desc"]
            if search_title:
                dic = {
                        "platform": self.platform,
                        "title": search_title,
                        "fetch_time": int(datetime.datetime.now().timestamp() * 1e3),

                }
                bulk_list.append(dic)
        hot_words_output_result(bulk_list)
        return True

    def search_page(self, title=None,*args,**kwargs):
        data_list = []
        timestamp = int(datetime.datetime.now().timestamp() * 1e3)
        url = "https://r.inews.qq.com/search?chlid=_qqnews_custom_search_all&search_from=&needSearchTabs=1&needSpreadAds=1&rtAd=1&new_user=0&uid=48a4725886d57203&omgid=&trueVersion=6.0.40&qimei=866174725888628&devid=866174725888628&appver=22_android_6.0.40&Cookie=lskey%3D;skey%3D;uin%3D;%20luin%3D;logintype%3D0;%20main_login%3D;%20&qn-sig=07db3b98ab9133d39b8b053fa1c51bd9&qn-rid=1002_2f55f6ab-2eb6-45e5-a4df-6dd9778c8b9d&qn-newsig=39b264b07173439d052ff2d6875cb7bc6aa47770dea55c7b64addee42138715a"
        post_json = {
                "search_type": "all",
                "query":title,
                "cp_type": "0",
                "disable_qc": "0",
                "searchStartFrom": "header",
                "launchSearchFrom": "billboard",
                "isDefault": "0",
                "searchTag": title,
                "adReqData": '{"adtype":0,"pf":"aphone","app_channel":"17","ext":{"mob":{"mobstr":"AdiIlDlcnKXLQu1Gx+HOa9fvgiA9BRLUAJ+RowxbYWkHaon9eDa0Qwt66FFNIY+xQHqSdGqfLc6p9ylswsJt1g4qWDeFDIxT6590GrPXznUizTPR0SutVVVQrHa1pbvX4WGx3yOrDNHGJCSrP38Gxej3\/ixgaVTB84d6i7sXgUhFCzcs3pS+DNShM79K7bIwO5U38eccvqle6nYKvELivuDIVr46chKdSokttQzbmf7OUSutGSHdn1+pihXvbFDkzgD+ut6PT\/G1E+O8eHwjZBf7K4Y8tpPABOH182j7JA6xpvoAP8r1WaHh73EtA5+T1M2dU3LtOMC0Sv\/Ngcf6btjefIkMDVoY+hWb8yKKd65UHSYvzpzLEdFNuEV8Sm33B789P9fCqLbnjf11OokPFjtC\/ORvR0dHItka56fkSNAZ2D+rmH8PPbMhZxSa\/bgOZywy2i8yu\/JRg8Rv8zRu4FkB6\/jIXkGCoWI1S7jUfnTIxCHu8iFOGo+Jr4VzMzqbnsi7XWhvKBye\/hPJkrISvw0wg5kg\/TPoj5Yu7aHH2pk31+uIbFRMFIzyj3p0I+yNmvpJECr4MuQmIXf8OP5OUlNVcDuZoXkyR4xy8ON1ou2Vtx+LQ\/x9xK2\/VR7up5apAPQMzmuzTOMcizdpO3FkrcXh0baOYJ7drGJWx4EO\/6nP9Y6J3GAU+YZsc+hCE3XHJpuZsfRsM2i7M4FnrZGz948VfFhY50Zk09eqK7y\/QsS++6su71tzvghFW0u3FOe1WMDvu3c4mMyYKIHkPQtGd5paAR81Xr6\/tGrhjh6CMcoHdppa9BV\/yM2s+NCTnxaZXoyuzljspI8x\/LjHLJuCLchAoPdOoND6mfoE7HGAajgdoFwR06I6zxN3RNQpB1RHIpmJCt+GcmAI4qld6qooO3lb\/8jkO8CBb69wapSAmvyzRvNVNPRa91ubAARkhW5DM62NjIDLN6COAWNEPZs6SfMbQ4jXNsIdXSR8ZZ8NuhO2uS9hU4+EadRYqVgn4yg1Z23d0HwQd0t0Gnw1X\/sAEIrR4sHyW0cVNMoWXkcfmM7UEq4oSCjLm6KTEhFuIR8EDm2HUEcUvcL+y0xr3Rr2YBuTVRR+bpnqffhYvyqRJILXaP2ddNrPt+a1Cl2sbL0INHVxfymPabok4Us8+jgbseBAf3iy8yOLDAQjG4z3iYVcLtgnoJnTLzTtAMC+wPYCbzoGi+hlXlBEF6FcxpU569ZT4YSIFI0xV8RXia+p7CnkaUWwmoKLBEwIG58rjqWO3+uyhvF0o\/\/RFi7QSF4U1DFy7qNQBPyoOiwEyKYZlbq4pQ6DjMYPWjBboU8NjY3qyoE\/CzwwSE75Gwk7w5DwYLs="}},"ver":"6.0.40","appversion":"200302","chid":2,"slot":[{"loid":"40,39","loid_watch_count":",0","channel":"_qqnews_custom_search_all","refresh_type":0,"recent_rot":["1,2,3"],"orders_info":["215508757,9693616,1554848392,1000,2801,110,2,CKsEMMrx8JcOOKTExfqioLSXG1AAYO32wqqapY+yEg==","215016046,9899501,1204054842,1000,4109,110,2,CKsEMMez\/wE4+fad\/47u\/sJLUNvVyZUNYKTQneXuxPaYngFyDAgBEI\/dwd33hde\/WXIECAIQAA==","214804999,14224364,2744407378,1000,606,110,2,CNkDMLuQydYFOJzXk9iVub73ZlC7rffuBGAAcgwIARDVn9eQtc2S6yNyBAgCEAA="]}],"launch":"0","wxversion":"0"}',
                "lon": "121.321859",
                "cityList": "news_news_sh",
                "loc_street": "申兰路",
                "village_name": "Unknown",
                "lastLocatingTime": str(int(timestamp/1e3)),
                "provinceId": "12",
                "loc_city_name": "上海市",
                "loc_catalog": "基础设施:交通设施:火车站",
                "loc_province_name": "上海市",
                "loc_name": "上海虹桥站",
                "town_name": "新虹街道",
                "loc_district_name": "闵行区",
                "loc_addr": "上海市闵行区申贵路1500号",
                "lat": "31.194424",
                "cityId": "12",
                "adcode": "310112",
                "is_special_device": "0",
                "mid": "0",
                "dpi": "320",
                "qqnetwork": "wifi",
                "rom_type": "R11-user 5.1.1 NMF26X 500200210 release-keys",
                "isColdLaunch": "1",
                "real_device_width": "2.81",
                "net_proxy": "DIRECT@",
                "net_bssid": "48:A4:72:58:86:D5",
                "isMainUserLogin": "0",
                "currentChannelId": "_qqnews_custom_search_all",
                "isElderMode": "0",
                "apptype": "android",
                "islite": "0",
                "hw": "OPPO_OPPOR11",
                "global_session_id": str(timestamp),
                "screen_width": "900",
                "isClosePersonalized": "0",
                "videoAutoPlay": "1",
                "imsi": "460077203886213",
                "cpuabi": "armeabi-v7a",
                "isoem": "0",
                "currentTabId": "news_news",
                "startTimestamp":  str(int(timestamp/1e3)),
                "net_slot": "0",
                "qn-time": str(timestamp),
                "pagestartfrom": "icon",
                "mac": "48:A4:72:58:86:D5",
                "activefrom": "icon",
                "net_ssid": "R1148a4725886d57203",
                "store": "17",
                "screen_height": "1600",
                "top_activity": "NewsSearchResultListActivity",
                "real_device_height": "5",
                "origin_imei": "866174725888628",
                "network_type": "wifi",
                "origCurrentTab": "top",
                "global_info": "1|1|1|1|1|14|4|1|0|6|1|1|1||0|J309P000000000:J902P000000000:J601P900000000:A601P800217702:A601P700321102:B601P600286205:A601P500154501:A601P400161601:J601P300000000:B601P200096102:A601P100272502:A601P000261102:J601P904000000:J601P903000000:A601P902266601:A601P901291001:J601P811000000:A601P701226201:A601P622269601:A601P621294101:A601P620269601:J601P111000000:J601P110000000:A601P109107102:A601P105118803:A601P019237403:A601P016212405:J601P006000000:J603P000000000:J401P100000000:A401P000050901:J602P900000000:J602P800000000:J602P700000000:J602P600000000:A602P500267502:B602P400286004:J602P300000000:J602P200000000:J602P100000000:B602P000315504:A602P901257901:J602P616000000:A602P615304801:A602P613271701:A602P611253801:A602P516234601:A602P414259901:A602P307160708:J602P302000000:A602P208205801:J602P117000000:A602P007272801:A602P003136401:J304P000000000:J310P700000000:A310P200210802:J310P100000000:B310P020314103:A310P010301701:B310P000267107:B701P000323002:A703P000322204:A704P000309801:J702P000000000:J405P000000000:J064P400000000:J064P300000000:B064P100243802:B064P020290902:J064P010000000:J064P000000000:A085P000087701:B074P200238202:J074P040000000:B074P030315703:A074P020315602:A074P010315401:B074P000142402:J903P000000000:A267P300215801:A267P200263601:A267P100299801:B267P000300102:A073P040317201:B073P030314503:A073P020313801:J073P010000000:B073P000313603:J060P700000000:J060P300000000:J060P200000000:B060P100299703:A060P090287301:J060P020000000:J060P010000000:B060P000311102:J060P099000000:J060P016000000:A406P000313203:J403P700000000:J403P600000000:A403P200206702:B403P100246105:J403P010000000:A403P000310401:A403P602218702:B404P200262402:A404P000263407:J055P200000000:J055P090000000:J055P080000000:J055P070000000:J055P060000000:J055P050000000:J055P010000000:A055P000265801:J402P100000000:J402P090000000:J402P080000000:J402P060000000:J402P020000000:A402P000301403:J054P400000000:J054P300000000:J054P200000000:A054P100269701:B054P090289604:A054P080289702:J054P050000000:J054P040000000:A054P030288501:J054P010000000:A054P000319901:J056P000000000:A901P200252304:B901P100226405:B901P000232405:J407P000000000|1402|0|1|25|25|0|0|0||3|3|1|1|1|1|1|1|-1|0|0|5|2|0|0|0|3|0|0|1|3|0|2|0|0|2|0|0|1|0|1|1|0|0|1|0|4|0|1|1|11|20|1|0|1|1|0|0|1|4|0|1|1|41|2|51|60|0|1|0|0|1|5|1|0|0|71|0|0|1|71",
                "imsi_history": "460077203886213",
                "net_apn": "0",

        }
        res = requests.post(url,headers=self.headers,data=post_json)
        page_text = res.json()
        for one_video in page_text["secList"]:
            video_dic = {}
            try:
                one_video = one_video["newsList"][0]
                video_dic['title'] = one_video.get('title')
                video_dic['url'] = one_video.get("url")
                releaser_id = one_video.get('media_id')
                video_dic['releaser'] = one_video.get('chlname')
                video_dic['releaserUrl'] = "https://view.inews.qq.com/media/%s" % releaser_id
                release_time = int(one_video.get('timestamp'))
                video_dic['release_time'] = int(release_time * 1e3)
                video_dic['video_id'] = one_video.get('video_channel').get("video").get("vid")
                video_dic['duration'] = trans_duration(one_video.get('video_channel').get("video").get("duration"))
                video_dic['play_count'] = one_video.get('readCount')
                video_dic['repost_count'] = one_video.get('shareCount')
                video_dic['comment_count'] = one_video.get('comments')
                video_dic['favorite_count'] = one_video.get('likeInfo')
                video_dic['fetch_time'] = int(datetime.datetime.now().timestamp() * 1e3)
                video_dic['releaser_id_str'] = "腾讯新闻_%s" % releaser_id
                video_dic['video_img'] = one_video.get('miniProShareImage')
                video_dic['platform'] = self.platform
                video_dic["is_hot"] = 1
                video_dic["data_provider"] = "CCR"
            except Exception as e:
                print(e)
                continue
            data_list.append(video_dic)
        output_result(result_Lst=data_list,
                      platform=self.platform,
                      output_to_es_raw=True,
                      )
        data_list.clear()


    def get_hot_videos(self, *args,**kwargs):
        self.search_page(*args,**kwargs)


if __name__ == "__main__":
    crawler = Crawler_Qq_News()
    # crawler.get_hot_words()
    crawler.search_page("11家方舱医院休仓")
