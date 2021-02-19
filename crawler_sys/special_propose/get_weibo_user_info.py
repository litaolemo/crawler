# -*- coding: utf-8 -*-
"""
Created on Thu May 31 09:20:25 2018

龟速爬取 需要提速

@author: fangyucheng
"""


from selenium import webdriver
import re
import json
import time



class Crawler_Get_Weibo_User_Info():
    
    
    def from_file_to_list(self,filename):
        openfile = open(filename)
        task = []
        for line in openfile:
            line_dic = line.replace('\n','')
            task.append(line_dic)
        return task
    
    
    
    def get_user_info(self,filename,resultname):
        result = open(resultname,'a')
        result_lst = []
        task_lst = self.from_file_to_list(filename)
        browser = webdriver.Chrome()
        for url in task_lst:
            try:
                browser.get(url)
                browser.maximize_window()
                time.sleep(8)
                user_name = browser.find_element_by_class_name('username').text
                print('get user_name')
                user_info = browser.find_element_by_class_name('info').text
                print('get user_info')
                try:
                    industry_detail_intro = browser.find_element_by_class_name('ul_detail').text
                    pattern1 = '行业类别'
                    pattern2 = '简介'
                    pattern3 = '毕业于'
                    try:
                        industry = ' '.join(re.findall('行业类别.*',industry_detail_intro)).replace(pattern1,'').replace(' ','')
                        print('get industry')
                    except:
                        industry = None
                    try:
                        detail_intro = ' '.join(re.findall('简介.*',industry_detail_intro)).replace(pattern2,'').replace(' ','').replace('：','')
                        print('get detail_intro')
                    except:
                        detail_intro = None
                    try:
                        graduated_from = ' '.join(re.findall('毕业于.*',industry_detail_intro)).replace(pattern3,'').replace(' ','')
                        print('get graduated_from')
                    except:
                        graduated_from = None
                except:
                    pass
                followers_fans = browser.find_element_by_class_name('tb_counter').text.split('\n')
                followers = followers_fans[0]
                print('get followers')
                fans = followers_fans[2]
                print('get fans')
                weibo_num = followers_fans[4]
                print('get weibo_num')
                D0 = {'user_name':user_name,
                      'user_info':user_info,
                      'industry':industry,
                      'detail_intro':detail_intro,
                      'followers':followers,
                      'fans':fans,
                      'weibo_num':weibo_num,
                      'url':url,
                      'graduated_from':graduated_from}
                print('get one user')
                result_lst.append(D0)
                json_D0 = json.dumps(D0)
                result.write(json_D0)
                result.write('\n')
                result.flush()
            except:
                pass
        return result_lst
    
    

if __name__=='__main__':
    test = Crawler_Get_Weibo_User_Info()
    filename='D:\CSM3.0\爬虫结果\weibo_user/weibo_user_list_try.txt'
    resultname='D:\CSM3.0\爬虫结果\weibo_user/weibo_user_info_20180531_2'
    firstV3 = test.get_user_info(filename,resultname)