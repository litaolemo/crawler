# -*- coding:utf-8 -*-
# @Time : 2020/3/3 14:51 
# @Author : litao
# -*- coding:utf-8 -*-
# @Time : 2020/3/2 16:37 
# @Author : litao
# -*- coding:utf-8 -*-
# @Time : 2020/3/2 11:07
# @Author : litao
# -*- coding:utf-8 -*-
# @Time : 2020/2/28 12:09
# @Author : litao


import requests
import json, re, datetime, urllib
from crawler.crawler_sys.utils.output_results import retry_get_url
from crawler.crawler_sys.utils.output_results import hot_words_output_result, output_result
from crawler.crawler_sys.utils.trans_strtime_to_timestamp import trans_strtime_to_timestamp
from crawler.crawler_sys.utils.trans_str_play_count_to_int import trans_play_count
from write_data_into_es.func_cal_doc_id import *
import random
from urllib.parse import parse_qs, urlparse


class Crawler_toutiao(object):
    def __init__(self):
        self.platform = "toutiao"

        self.headers = {

                "Host": "i.snssdk.com",
                "Connection": "keep-alive",
                "Accept": "application/json, text/javascript",
                "X-Requested-With": "XMLHttpRequest",
                "User-Agent": "Mozilla/5.0 (Linux; Android 5.1.1; OPPO R11 Build/NMF26X; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/74.0.3729.136 Mobile Safari/537.36 JsSdk/2 NewsArticle/7.6.3 NetType/wifi",
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": "https://i.snssdk.com/feoffline/hot_list/template/hot_list/?fe_api_version=2&is_in_channel=1&count=50&fe_source=hot_board&tab_name=tab_hot_board&is_web_refresh=1&style_type=18&client_extra_params=%7B%22hot_board_source%22%3A%22hot_board%22%7D&extra=%7B%22CardStyle%22%3A0%2C%22JumpToWebList%22%3Atrue%7D&category=hot_board&stream_api_version=88&tt_daymode=1&iid=105857671701&device_id=70787469432&ac=wifi&mac_address=48%3AA4%3A72%3A58%3A86%3AD5&channel=store_yingyonghui_0107&aid=13&app_name=news_article&version_code=763&version_name=7.6.3&device_platform=android&ab_version=801968%2C1419043%2C668775%2C1462526%2C1512584%2C1190522%2C1489307%2C1157750%2C1157634%2C1419598%2C1493796%2C1439625%2C1469498%2C668779%2C1417597%2C662099%2C1403340%2C668774%2C1509255%2C1396151%2C821967%2C857803%2C660830%2C1434501%2C662176%2C1491631&ab_feature=102749%2C94563&device_type=OPPO+R11&device_brand=OPPO&language=zh&os_api=22&os_version=5.1.1&uuid=866174725888628&openudid=48a4725886d57203&manifest_version_code=7630&resolution=900*1600&dpi=320&update_version_code=76309&_rticket=1583217846540&plugin=0&tma_jssdk_version=1.54.0.3&rom_version=coloros__r11-user+5.1.1+nmf26x+500200210+release-keys&cdid=754b9ff9-5880-48b2-ac40-3880effd3f33",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",

        }

    def get_hot_words(self):
        bulk_list = []
        url = "https://i.snssdk.com/api/feed/hotboard_online/v1/?fe_api_version=2&is_in_channel=1&count=50&fe_source=hot_board&tab_name=tab_hot_board&is_web_refresh=1&style_type=18&client_extra_params=%7B%22hot_board_source%22%3A%22hot_board%22%2C%22fe_version%22%3A%22v11%22%7D&extra=%7B%22CardStyle%22%3A0%2C%22JumpToWebList%22%3Atrue%7D&category=hotboard_online&stream_api_version=88&tt_daymode=1&iid=105857671701&device_id=70787469432&ac=wifi&mac_address=48%3AA4%3A72%3A58%3A86%3AD5&channel=store_yingyonghui_0107&aid=13&app_name=news_article&version_code=763&version_name=7.6.3&device_platform=android&ab_version=801968%2C1419043%2C668775%2C1462526%2C1512584%2C1190522%2C1489307%2C1157750%2C1157634%2C1419598%2C1493796%2C1439625%2C1469498%2C668779%2C1417597%2C662099%2C1403340%2C668774%2C1509255%2C1396151%2C821967%2C857803%2C660830%2C1434501%2C662176%2C1491631&ab_feature=102749%2C94563&device_type=OPPO%2BR11&device_brand=OPPO&language=zh&os_api=22&os_version=5.1.1&uuid=866174725888628&openudid=48a4725886d57203&manifest_version_code=7630&resolution=900*1600&dpi=320&update_version_code=76309&plugin=0&tma_jssdk_version=1.54.0.3&rom_version=coloros__r11-user%2B5.1.1%2Bnmf26x%2B500200210%2Brelease-keys&cdid=754b9ff9-5880-48b2-ac40-3880effd3f33"
        page_res = retry_get_url(url, headers=self.headers, proxies=3, timeout=5)
        page_json = page_res.json()
        for data in page_json["data"]:
            contect = data["content"]
            data = json.loads(contect)
            schema = data["raw_data"]["schema"]
            # search_str = urllib.parse.unquote(schema)
            query = urlparse(schema).query  # wd=python&ie=utf-8
            params = parse_qs(query)  # {'wd': ['python'], 'ie': ['utf-8']}
            """所得的字典的value都是以列表的形式存在，若列表中都只有一个值"""
            result = {key: params[key][0] for key in params}
            search_title = result.get("keyword")
            # search_json = result.get("search_json")
            if search_title:
                dic = {
                        "platform": self.platform,
                        "title": search_title,
                        "fetch_time": int(datetime.datetime.now().timestamp() * 1e3),
                        "search_json": schema
                }
                bulk_list.append(dic)
        hot_words_output_result(bulk_list)
        return True

    def search_page(self, title=None, search_json=None, **kwargs):
        data_list = []
        timestamp = int(datetime.datetime.now().timestamp() * 1e3)
        title = urllib.parse.quote(title)
        headers = {
                "Accept-Encoding": "gzip",
                # "X-SS-REQ-TICKET": "1587102750860",
                "passport-sdk-version": "14",
                "sdk-version": "2",
                #"Cookie": "odin_tt=d5d96b2812637e9d20681530fbbe4d52e8f76ae1b6afa8c0a173260321611c507ac6eca10991b21fc4f023e94371d457df784f959e94db673ef29a5bd2137091; qh[360]=1; history=alrvlFic6pJZXJCTWBmSmZt6KW6mevZSz5LU3OJ7DEKX42Zw%2Bc84wMR3iYGBweFy3EzZsPcNTLyXWN1AvLYP8%2BQPMLFfEpUA8bo%2F7nNtYOK7xNwC4k3XmMHe5MtzSTiM48DluNr01dkNTDyXuHrApsi4ejkwsV%2BSmAPmSeXoMzDxXhKcAuIVrRfWAJnJJwA25fG1DoezvFBTZrzZeg6kT%2BwWSG7Gx3UJB5h4L%2FH4gXlVn%2BtAtkvFMQRcjpv%2B%2Be9TBib2S%2BwcYBuUn8xsYGK%2FJKMAkptgfXrDASaOS4yHQHJVPy6UOjDxXuI4BeJN26Fs6MDEcYn%2FEoMDAAAA%2F%2F8%3D; install_id=112651077855; ttreq=1$0b37d53ca5c301ce96959dc97a67886da420b294",
                # "X-Gorgon": "0401007140017aae019cc2020b1c48dbab0ba42839014487648a",
                #"X-Khronos": "1587102750",
                "Host": "is.snssdk.com",
                "Connection": "Keep-Alive",
                "User-Agent": "okhttp/3.10.0.1",
        }
        url = "https://is.snssdk.com/api/search/content/?os_api=23&device_type=oneplus+a5010&from_search_subtab=synthesis&manifest_version_code=7690&source=search_subtab_switch&offset=0&is_ttwebview=0&action_type&is_incognito=0&keyword_type&rom_version=23&app_name=news_article&format=json&version_name=7.6.9&ac=wifi&host_abi=armeabi-v7a&update_version_code=76909&channel=baidu_0411&is_native_req=1&loadId=1&longitude=116.40717530841052&isIncognito=0&plugin=2050&forum=1&latitude=39.904680919672145&language=zh&pd=video&cur_tab_title=search_tab&aid=13&dpi=270&qrecImprId&fetch_by_ttnet=1&count=10&plugin_enable=3&search_position&ab_group=100167%2C94569%2C102754&keyword={0}&scm_version=1.0.2.830&search_json=%7B%22comment_ids%22%3A%5B%5D%2C%22event_discussion%22%3A74123%2C%22event_impression%22%3A17270790%2C%22forum_id%22%3A1664181806902302%2C%22forum_recall_wtt%22%3A%5B1664190666034183%2C1664192273575943%2C1664184430218253%2C1664185769175051%2C1664184985139212%2C1664196237152267%2C1664186792648732%2C1664188755414019%2C1664187055838215%2C1664184182571022%2C1664185938950148%2C1664188041995268%2C1664188322863172%2C1664190185024520%2C1664185602828300%2C1664184276484099%2C1664188211399684%2C1664187870713868%2C1664184484958211%2C1664183864289288%2C1664186825487371%2C1664195548700686%2C1664186585780228%2C1664197296210947%2C1664188146725901%2C1664191748459523%5D%2C%22group_source%22%3Anull%2C%22hot_gid%22%3A6816255461172445703%2C%22log_pb%22%3A%7B%22cluster_type%22%3A%220%22%2C%22entrance_hotspot%22%3A%22channel%22%2C%22hot_board_cluster_id%22%3A%226816091697949180424%22%2C%22hot_board_impr_id%22%3A%22202004171352010100140411610B1A7741%22%2C%22location%22%3A%22hot_board%22%2C%22rank%22%3A%225%22%2C%22source%22%3A%22trending_tab%22%2C%22style_id%22%3A%2210005%22%7D%2C%22mix_stick_ids%22%3A%5B1664190666034183%2C1664192273575943%2C1664184430218253%2C1664185769175051%2C1664184985139212%2C1664196237152267%2C1664186792648732%2C1664188755414019%2C1664187055838215%2C1664184182571022%2C1664185938950148%2C1664188041995268%2C1664188322863172%2C1664190185024520%2C1664185602828300%2C1664184276484099%2C1664188211399684%2C1664187870713868%2C1664184484958211%2C1664183864289288%2C1664186825487371%2C1664195548700686%2C1664186585780228%2C1664197296210947%2C1664188146725901%2C1664191748459523%5D%2C%22stick_group_ids%22%3A%5B%5D%7D&device_platform=android&search_id&has_count=0&version_code=769&from=video&device_id={1}&resolution=1080*1920&os_version=6.0.1&device_brand=Oneplus&search_sug=1&qc_query".format(
                title,random.randint(69418800000,69418899999))
        res = retry_get_url(url, headers=headers, timeout=5, proxies=3)
        page_text = res.json()
        for one_video in page_text["data"]:
            video_dic = {}
            try:
                video_dic['title'] = one_video.get('title')
                video_dic['url'] = one_video.get('display').get("info").get("url")
                releaser_id = re.findall("user_id=(\d+)", one_video.get('user_source_url'))[0]
                video_dic['releaser'] = one_video.get('media_name')
                video_dic['releaserUrl'] = "https://www.toutiao.com/c/user/%s/" % releaser_id
                release_time = int(one_video.get('create_time'))
                video_dic['release_time'] = int(release_time * 1e3)
                video_dic['duration'] = int(one_video.get('video_duration'))
                video_dic['play_count'] = trans_play_count(one_video.get('play_effective_count'))
                video_dic['repost_count'] = 0
                video_dic['comment_count'] = one_video.get('comment_count')
                video_dic['favorite_count'] = one_video.get('digg_count')
                video_dic['fetch_time'] = int(datetime.datetime.now().timestamp() * 1e3)
                video_dic['releaser_id_str'] = "toutiao_%s" % releaser_id
                video_dic['video_img'] = one_video.get('display').get('self_info').get('image_url')
                video_dic['platform'] = "toutiao"
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

    def search_short_video_page(self, title=None, search_json=None, **kwargs):
        data_list = []
        timestamp = int(datetime.datetime.now().timestamp() * 1e3)
        title = urllib.parse.quote(title)
        headers = {

                "Accept-Encoding": "gzip",
                # "X-SS-REQ-TICKET": "1587103224961",
                "passport-sdk-version": "14",
                "sdk-version": "2",
                #"Cookie": "odin_tt=d5d96b2812637e9d20681530fbbe4d52e8f76ae1b6afa8c0a173260321611c507ac6eca10991b21fc4f023e94371d457df784f959e94db673ef29a5bd2137091; qh[360]=1; history=alrvlFic6pJZXJCTWBmSmZt6KW6mevZSz5LU3OJ7DEKX42Zw%2Bc84wMR3iYGBweFy3EzZsPcNTLyXWN1AvLYP8%2BQPMLFfEpUA8bo%2F7nNtYOK7xNwC4k3XmMHe5MtzSTiM48DluNr01dkNTDyXuHrApsi4ejkwsV%2BSmAPmSeXoMzDxXhKcAuIVrRfWAJnJJwA25fG1DoezvFBTZrzZeg6kT%2BwWSG7Gx3UJB5h4L%2FH4gXlVn%2BtAtkvFMQRcjpv%2B%2Be9TBib2S%2BwcYBuUn8xsYGK%2FJKMAkptgfXrDASaOS4yHQHJVPy6UOjDxXuI4BeJN26Fs6MDEcYn%2FEoMDAAAA%2F%2F8%3D; install_id=112651077855; ttreq=1$0b37d53ca5c301ce96959dc97a67886da420b294",
                # "X-Gorgon": "0401e08b4001a628dcf96b16d01278ad842e915d905b213dc48f",
                # "X-Khronos": "1587103224",
                "Host": "is.snssdk.com",
                "Connection": "Keep-Alive",
                "User-Agent": "okhttp/3.10.0.1",

        }
        url = "https://is.snssdk.com/api/search/content/?os_api=23&device_type=oneplus%2Ba5010&from_search_subtab=video&manifest_version_code=7690&source=search_subtab_switch&offset=0&is_ttwebview=0&uuid=440000000189785&action_type&is_incognito=0&keyword_type&rom_version=23&app_name=news_article&format=json&version_name=7.6.9&ac=wifi&host_abi=armeabi-v7a&update_version_code=76909&channel=baidu_0411&is_native_req=1&loadId=1&longitude=113.40717530841052&isIncognito=0&plugin=2050&openudid=e44cc0264b92bcbf&forum=1&latitude=39.904680919672145&search_start_time=1587102733626&language=zh&pd=xiaoshipin&cur_tab_title=search_tab&aid=13&pos=5r_-9Onkv6e_eBEKeScxeCUfv7G_8fLz-vTp6Pn4v6esrKuzqa2qrKqorq2lqaytqK-xv_H86fTp6Pn4v6eupLOkramrpa2krKSrqq-sqaixv_zw_O3e9Onkv6e_eBEKeScxeCUfv7G__PD87dHy8_r06ej5-L-nrKyrs6mtqqyqqK6tpamsraivsb_88Pzt0fzp9Ono-fi_p66ks6StqaulraSspKuqr6ypqOA%253D&dpi=270&qrecImprId&fetch_by_ttnet=1&count=10&plugin_enable=3&search_position&ab_group=100167%252C94569%252C102754&keyword={0}&scm_version=1.0.2.830&search_json=%257B%2522comment_ids%2522%253A%255B%255D%252C%2522event_discussion%2522%253A74123%252C%2522event_impression%2522%253A17270790%252C%2522forum_id%2522%253A1664181806902302%252C%2522forum_recall_wtt%2522%253A%255B1664190666034183%252C1664192273575943%252C1664184430218253%252C1664185769175051%252C1664184985139212%252C1664196237152267%252C1664186792648732%252C1664188755414019%252C1664187055838215%252C1664184182571022%252C1664185938950148%252C1664188041995268%252C1664188322863172%252C1664190185024520%252C1664185602828300%252C1664184276484099%252C1664188211399684%252C1664187870713868%252C1664184484958211%252C1664183864289288%252C1664186825487371%252C1664195548700686%252C1664186585780228%252C1664197296210947%252C1664188146725901%252C1664191748459523%255D%252C%2522group_source%2522%253Anull%252C%2522hot_gid%2522%253A6816255461172445703%252C%2522log_pb%2522%253A%257B%2522cluster_type%2522%253A%25220%2522%252C%2522entrance_hotspot%2522%253A%2522channel%2522%252C%2522hot_board_cluster_id%2522%253A%25226816091697949180424%2522%252C%2522hot_board_impr_id%2522%253A%2522202004171352010100140411610B1A7741%2522%252C%2522location%2522%253A%2522hot_board%2522%252C%2522rank%2522%253A%25225%2522%252C%2522source%2522%253A%2522trending_tab%2522%252C%2522style_id%2522%253A%252210005%2522%257D%252C%2522mix_stick_ids%2522%253A%255B1664190666034183%252C1664192273575943%252C1664184430218253%252C1664185769175051%252C1664184985139212%252C1664196237152267%252C1664186792648732%252C1664188755414019%252C1664187055838215%252C1664184182571022%252C1664185938950148%252C1664188041995268%252C1664188322863172%252C1664190185024520%252C1664185602828300%252C1664184276484099%252C1664188211399684%252C1664187870713868%252C1664184484958211%252C1664183864289288%252C1664186825487371%252C1664195548700686%252C1664186585780228%252C1664197296210947%252C1664188146725901%252C1664191748459523%255D%252C%2522stick_group_ids%2522%253A%255B%255D%257D&device_platform=android&search_id&has_count=0&version_code=769&mac_address=08%253A00%253A27%253A1F%253A7E%253AA0&from=xiaoshipin&device_id={1}&resolution=810*1440&os_version=6.0.1&device_brand=Oneplus&search_sug=1&qc_query".format(
                title,random.randint(69418800000,69418899999))

        res = retry_get_url(url, headers=headers, timeout=5, proxies=3)
        page_text = res.json()
        for one_video in page_text["data"]:
            video_dic = {}
            try:
                one_video = one_video["raw_data"]
                video_dic['title'] = one_video.get('title')
                video_dic['url'] = one_video.get('share').get("share_url")
                releaser_id = one_video.get('user').get("info").get("user_id")
                video_dic['releaser'] = one_video.get('user').get("info").get("name")
                video_dic['releaserUrl'] = "https://www.toutiao.com/c/user/%s/" % releaser_id
                release_time = int(one_video.get('create_time'))
                video_dic['release_time'] = int(release_time * 1e3)
                video_dic['duration'] = int(one_video.get('video').get("duration"))
                video_dic['play_count'] = one_video.get('action').get("play_count")
                video_dic['repost_count'] = one_video.get('action').get("share_count")
                video_dic['comment_count'] = one_video.get('action').get("comment_count")
                video_dic['favorite_count'] = one_video.get('action').get("digg_count")
                video_dic['fetch_time'] = int(datetime.datetime.now().timestamp() * 1e3)
                video_dic['releaser_id_str'] = "toutiao_%s" % releaser_id
                video_dic['video_img'] = one_video.get('video').get('origin_cover').get('url_list')[0]
                video_dic['platform'] = "toutiao"
                if "iesdouyin" in video_dic['url']:
                    video_dic['releaserUrl'] = "https://www.douyin.com/share/user/%s/" % releaser_id
                    video_dic['platform'] = "抖音"
                    video_dic['releaser_id_str'] = "抖音_%s" % releaser_id
                    video_dic['play_count'] = 0
                video_dic["is_hot"] = 1
                video_dic["data_provider"] = "CCR"
            except:
                continue
            data_list.append(video_dic)
        output_result(result_Lst=data_list,
                      platform=self.platform,
                      output_to_es_raw=True,
                      )
        data_list.clear()

    def get_hot_videos(self, *args, **kwargs):
        self.search_page(*args, **kwargs)
        self.search_short_video_page(*args, **kwargs)


if __name__ == "__main__":
    crawler = Crawler_toutiao()
    crawler.get_hot_words()
    # crawler.search_page("山东 火线提拔",
    #                     'sslocal://search?disable_record_history=true&from=trending_tab&hot_board_cluster_id=1654391459809283&hot_board_impr_id=202003031451280100150450182605F63D&keyword=%23%E5%B1%B1%E4%B8%9C%20%E7%81%AB%E7%BA%BF%E6%8F%90%E6%8B%94%23&search_json=%7B%22comment_ids%22%3A%5B%5D%2C%22event_discussion%22%3A116635%2C%22event_impression%22%3A27176007%2C%22forum_id%22%3A1660114103413768%2C%22forum_recall_wtt%22%3A%5B1660117192554499%2C1660119602787340%2C1660116731441155%2C1660122355505164%2C1660117380056075%2C1660120151918599%2C1660120404429828%2C1660119017074695%2C1660122046612493%2C1660124327045131%2C1660116231574535%5D%2C%22group_source%22%3Anull%2C%22hot_gid%22%3A6799674340544610823%2C%22log_pb%22%3A%7B%22hot_board_cluster_id%22%3A%221654391459809283%22%2C%22hot_board_impr_id%22%3A%22202003031451280100150450182605F63D%22%2C%22source%22%3A%22trending_tab%22%7D%2C%22mix_stick_ids%22%3A%5B1660117192554499%2C1660119602787340%2C1660116731441155%2C1660122355505164%2C1660117380056075%2C1660120151918599%2C1660120404429828%2C1660119017074695%2C1660122046612493%2C1660124327045131%2C1660116231574535%5D%2C%22stick_group_ids%22%3A%5B%5D%7D&source=trending_tab')
    # crawler.search_short_video_page("山东 火线提拔",
    #                     'sslocal://search?disable_record_history=true&from=trending_tab&hot_board_cluster_id=1654391459809283&hot_board_impr_id=202003031451280100150450182605F63D&keyword=%23%E5%B1%B1%E4%B8%9C%20%E7%81%AB%E7%BA%BF%E6%8F%90%E6%8B%94%23&search_json=%7B%22comment_ids%22%3A%5B%5D%2C%22event_discussion%22%3A116635%2C%22event_impression%22%3A27176007%2C%22forum_id%22%3A1660114103413768%2C%22forum_recall_wtt%22%3A%5B1660117192554499%2C1660119602787340%2C1660116731441155%2C1660122355505164%2C1660117380056075%2C1660120151918599%2C1660120404429828%2C1660119017074695%2C1660122046612493%2C1660124327045131%2C1660116231574535%5D%2C%22group_source%22%3Anull%2C%22hot_gid%22%3A6799674340544610823%2C%22log_pb%22%3A%7B%22hot_board_cluster_id%22%3A%221654391459809283%22%2C%22hot_board_impr_id%22%3A%22202003031451280100150450182605F63D%22%2C%22source%22%3A%22trending_tab%22%7D%2C%22mix_stick_ids%22%3A%5B1660117192554499%2C1660119602787340%2C1660116731441155%2C1660122355505164%2C1660117380056075%2C1660120151918599%2C1660120404429828%2C1660119017074695%2C1660122046612493%2C1660124327045131%2C1660116231574535%5D%2C%22stick_group_ids%22%3A%5B%5D%7D&source=trending_tab')
