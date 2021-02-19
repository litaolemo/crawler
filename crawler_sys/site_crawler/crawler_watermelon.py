# -*- coding: utf-8 -*-
"""
Created on Mon May 28 10:29:57 2018

@author: fangyucheng
"""




import requests
import json
import datetime
import re
from framework.video_fields_std import Std_fields_video
#from . import bulk_write_into_es
import js2py
import hashlib
import time
from selenium import webdriver


class Crawler_Watermelon(Std_fields_video):

    def write_into_file(self, data_dict, file_obj):
        json_str=json.dumps(data_dict)
        file_obj.write(json_str)
        file_obj.write('\n')
        file_obj.flush()  
        
        
    def feed_url_into_redis(self, dict_Lst):
        pass
    
    
    def output_result(self, result_Lst, output_to_file=False, filepath=None):
        # write data into es crawler-raw index
        #bulk_write_into_es(result_Lst)

        # feed url into redis 
        self.feed_url_into_redis(result_Lst)
        
        # output into file according to passed in parameters
        if output_to_file==True and filepath!=None:
            output_fn='crawler_watermelon_%s_json' % datetime.datetime.now().isoformat()[:10]
            output_f=open(filepath+'/'+output_fn, 'a', encoding='utf-8')
            self.write_into_file(result_Lst, output_f)
        else:
            pass
    
    
    def get_list_video(self,output_to_file=False, filepath=None):
        result_Lst = []
        max_behot_time = 0
        count = 0
        
        headers = {'Host': 'ic.snssdk.com',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                   'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                   'Accept-Encoding': 'gzip, deflate',
                   'Cookie': 'odin_tt=5b54e47f71b1963502fe03c4028f5672c887a0b739ce2302481beda2a4388a0a538ade820b54b4589da13d18dde9d245',
                   'Connection': 'keep-alive',
                   'Upgrade-Insecure-Requests': '1',
                   'Cache-Control': 'max-age=0'}
        
        
        while count <= 0:
            time_now = int(time.time())
            listurl = 'http://ic.snssdk.com/video/app/stream/v51/?category=subv_xg_society&refer=1&count=20&max_behot_time='+str(max_behot_time)+'&list_entrance=main_tab&last_refresh_sub_entrance_interval='+str(time_now)
                       #http://ic.snssdk.com/video/app/stream/v51/?category=subv_xg_society&refer=1&count=20&list_entrance=main_tab&last_refresh_sub_entrance_interval=1527473360&loc_mode=5&tt_from=refresh_auto&play_param=codec_type%3A0&iid=33815381012&device_id=52965120460&ac=wifi&channel=wandoujia&aid=32&app_name=video_article&version_code=653&version_name=6.5.3&device_platform=android&ab_version=359940%2C344692%2C353539%2C356329%2C361439%2C324397%2C361311%2C358091%2C358364%2C356602%2C350431%2C354439%2C325211%2C346575%2C342302%2C361530%2C320651%2C361551&ssmix=a&device_type=MuMu&device_brand=Android&language=zh&os_api=19&os_version=4.4.4&uuid=008796749793280&openudid=54767d8bf41ac9a4&manifest_version_code=253&resolution=1280*720&dpi=240&update_version_code=65307&_rticket=1527473360674&rom_version=cancro-eng+4.4.4+V417IR+eng.root.20180201.174500+release-keys&fp=i2T_FYmuPzL5Fl4ZcrU1FYFeL2FW
            
            get_page = requests.get(listurl,headers=headers)
            page = get_page.text
            page = page.replace('true','True')
            page = page.replace('false','False')
            page = page.replace('null','"Null"')
            page_dic = eval(page)
            video_agg = page_dic['data']
            count += 1
            for line in video_agg:
                try:
                    video_str=line['content']
                    video_dic=eval(video_str)
                    if video_dic['has_video']==True:
                        title = video_dic['title']
                        url = video_dic['display_url']
                        browser = webdriver.Chrome()
                        browser.get(url)
                        pc_midstep = browser.find_element_by_class_name('num').text
                        play_count = ' '.join(re.findall('\d+',pc_midstep))

                        release_time = int(video_dic['publish_time']*1e3)
                        play_count2 = video_dic['read_count']
                        releaser = video_dic['media_name']
                        max_behot_time = video_dic['behot_time']
                        video_id = video_dic['item_id']
                        releaser_id = video_dic['user_info']['user_id']
                        fetch_time = int(datetime.datetime.now().timestamp()*1e3)
                        
                        D0={'title':title,'url':url,'release_time':release_time,'releaser':releaser,'play_count':play_count,
                            'video_id':video_id,'releaser_id':releaser_id,'fetch_time':fetch_time,'play_count2':play_count2}
                            
                        result_Lst.append(D0)
                        print ('get one video')
                except:
                    pass
            browser.close()
        self.output_result(result_Lst,output_to_file=output_to_file,filepath=filepath)
        return result_Lst
            #result_Lst.clear()

if __name__=='__main__':
    test=Crawler_Watermelon()
    output_to_file = True
    filepath = 'D:/CSM3.0/爬虫结果/watermelon'
    gogogo = test.get_list_video(output_to_file,filepath)