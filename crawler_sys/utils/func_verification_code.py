# -*- coding:utf-8 -*-
# @Time : 2019/11/20 13:52
# @Author : litao
import numpy as np
import random

import json,redis,re,requests
from selenium.webdriver import ActionChains
import time,datetime
from selenium import webdriver
from PIL import Image
import os
from selenium.webdriver.support.ui import WebDriverWait
import cv2
from fontTools.ttLib import *


class Login(object):
    """
    腾讯防水墙滑动验证码破解
    使用OpenCV库
    成功率大概90%左右：在实际应用中，登录后可判断当前页面是否有登录成功才会出现的信息：比如用户名等。循环
    https://open.captcha.qq.com/online.html
    破解 腾讯滑动验证码
    腾讯防水墙
    python + seleniuum + cv2
    """

    rds = redis.StrictRedis(host='192.168.17.60', port=6379, db=2, decode_responses=True)
    # chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument('--headless')
    # chrome_options.add_argument('--disable-gpu')
    # # self.chrome_options.add_argument("--start-maximized")
    # chrome_options.add_argument("--no-sandbox")
    # chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
    # timestamp = str(datetime.datetime.now().timestamp() * 1e3)
    # first_page_headers = {
    #         "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
    #         "Accept-Encoding": "gzip, deflate, br",
    #         "Accept-Language": "zh-CN,zh;q=0.9",
    #         "Cache-Control": "max-age=0",
    #         "Connection": "keep-alive",
    #         "Host": "live.kuaishou.com",
    #         "Upgrade-Insecure-Requests": "1",
    #         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36",
    # }

    def __init__(self):
        # 如果是实际应用中，可在此处账号和密码
        # self.url = "https://open.captcha.qq.com/online.html"
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--disable-gpu')
        # self.chrome_options.add_argument("--start-maximized")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        self.timestamp = str(datetime.datetime.now().timestamp()*1e3)
        self.first_page_headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Cache-Control": "max-age=0",
                "Connection": "keep-alive",
                "Host": "live.kuaishou.com",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36",
        }
        # self.rds = redis.StrictRedis(host='192.168.17.60', port=6379, db=2, decode_responses=True)


    @staticmethod
    def show(name):
        cv2.imshow('Show', name)
        cv2.waitKey(0)
        cv2.destroyAllWindows()

    @staticmethod
    def webdriverwait_send_keys(dri, element, value):
        """
        显示等待输入
        :param dri: driver
        :param element:
        :param value:
        :return:
        """
        WebDriverWait(dri, 10, 5).until(lambda dr: element).send_keys(value)

    @staticmethod
    def webdriverwait_click(dri, element):
        """
        显示等待 click
        :param dri: driver
        :param element:
        :return:
        """
        WebDriverWait(dri, 10, 5).until(lambda dr: element).click()


    def get_postion(self,chunk, canves):
        """
        判断缺口位置
        :param chunk: 缺口图片是原图
        :param canves:
        :return: 位置 x, y
        """
        otemp = chunk
        oblk = canves
        target = cv2.imread(otemp, 0)
        template = cv2.imread(oblk, 0)
        # w, h = target.shape[::-1]
        temp = './image/temp_%s.jpg' % self.timestamp
        targ = './image/targ_%s.jpg' % self.timestamp
        cv2.imwrite(temp, template)
        cv2.imwrite(targ, target)
        target = cv2.imread(targ)
        target = cv2.cvtColor(target, cv2.COLOR_BGR2GRAY)
        target = abs(255 - target)
        cv2.imwrite(targ, target)
        target = cv2.imread(targ)
        template = cv2.imread(temp)
        result = cv2.matchTemplate(target, template, cv2.TM_CCOEFF_NORMED)
        x, y = np.unravel_index(result.argmax(), result.shape)
        return x, y
        # # 展示圈出来的区域
        # cv2.rectangle(template, (y, x), (y + w, x + h), (7, 249, 151), 2)
        # cv2.imwrite("yuantu.jpg", template)
        # show(template)

    @staticmethod
    def get_track(distance):
        """
        模拟轨迹 假装是人在操作
        :param distance:
        :return:
        """
        # 初速度
        v = 0
        # 单位时间为0.2s来统计轨迹，轨迹即0.2内的位移
        t = 0.2
        # 位移/轨迹列表，列表内的一个元素代表0.2s的位移
        tracks = []
        # 当前的位移
        current = 0
        # 到达mid值开始减速
        mid = distance * 7 / 8

        distance += 10  # 先滑过一点，最后再反着滑动回来
        # a = random.randint(1,3)
        while current < distance:
            if current < mid:
                # 加速度越小，单位时间的位移越小,模拟的轨迹就越多越详细
                a = random.randint(2, 4)  # 加速运动
            else:
                a = -random.randint(3, 5)  # 减速运动

            # 初速度
            v0 = v
            # 0.2秒时间内的位移
            s = v0 * t + 0.5 * a * (t ** 2)
            # 当前的位置
            current += s
            # 添加到轨迹列表
            tracks.append(round(s))

            # 速度已经达到v,该速度作为下次的初速度
            v = v0 + a * t

        # 反着滑动到大概准确位置
        for i in range(4):
            tracks.append(-random.randint(2, 3))
        for i in range(4):
            tracks.append(-random.randint(1, 3))
        return tracks



    @staticmethod
    def urllib_download(imgurl, imgsavepath):
        """
        下载图片
        :param imgurl: 图片url
        :param imgsavepath: 存放地址
        :return:
        """
        from urllib.request import urlretrieve
        urlretrieve(imgurl, imgsavepath)

    def after_quit(self):
        """
        关闭浏览器
        :return:
        """
        self.driver.quit()

    def get_num_dic(self):
        xml_re = {
                '<TTGlyph name="(.*)" xMin="32" yMin="-6" xMax="526" yMax="729">': 0,
                '<TTGlyph name="(.*)" xMin="32" yMin="7" xMax="526" yMax="742">': 0,
                '<TTGlyph name="(.*)" xMin="98" yMin="13" xMax="363" yMax="726">': 1,
                '<TTGlyph name="(.*)" xMin="98" yMin="26" xMax="363" yMax="739">': 1,
                '<TTGlyph name="(.*)" xMin="32" yMin="13" xMax="527" yMax="732">': 2,
                '<TTGlyph name="(.*)" xMin="32" yMin="26" xMax="527" yMax="745">': 2,
                '<TTGlyph name="(.*)" xMin="25" yMin="-6" xMax="525" yMax="730">': 3,
                '<TTGlyph name="(.*)" xMin="25" yMin="7" xMax="525" yMax="743">': 3,
                '<TTGlyph name="(.*)" xMin="26" yMin="13" xMax="536" yMax="731">': 4,
                '<TTGlyph name="(.*)" xMin="26" yMin="26" xMax="536" yMax="744">': 4,
                '<TTGlyph name="(.*)" xMin="33" yMin="-5" xMax="526" yMax="717">': 5,
                '<TTGlyph name="(.*)" xMin="33" yMin="8" xMax="526" yMax="730">': 5,
                '<TTGlyph name="(.*)" xMin="39" yMin="-5" xMax="530" yMax="732">': 6,
                '<TTGlyph name="(.*)" xMin="39" yMin="8" xMax="530" yMax="745">': 6,
                '<TTGlyph name="(.*)" xMin="38" yMin="13" xMax="536" yMax="717">': 7,
                '<TTGlyph name="(.*)" xMin="38" yMin="26" xMax="536" yMax="730">': 7,
                '<TTGlyph name="(.*)" xMin="33" yMin="-7" xMax="525" yMax="731">': 8,
                '<TTGlyph name="(.*)" xMin="33" yMin="6" xMax="525" yMax="744">': 8,
                '<TTGlyph name="(.*)" xMin="37" yMin="-7" xMax="521" yMax="730">': 9,
                '<TTGlyph name="(.*)" xMin="37" yMin="6" xMax="521" yMax="743">': 9
        }
        uni_code_dic = {}
        try:
            for re_code in xml_re:
                code_dic = re.findall(re_code, self.xml_text)
                if code_dic:
                    uni_code_dic[code_dic[0].replace("uni", "\\\\u").lower()] = xml_re[re_code]
            # print("uni_code_dic", uni_code_dic)
            return uni_code_dic
        except:
            print("can't find ",self.xml_text)
            return False

    def unicode_to_num(self, uni_str):
        count_num = str(uni_str.encode("unicode_escape"))[2:-1]
        # print(count_num)
        for i in self.uni_code_dic:
            if i in count_num:
                count_num = count_num.replace(i, str(self.uni_code_dic[i]))
        # print(count_num)
        return count_num

    def login_main(self,url):

        driver = self.driver
        driver.maximize_window()
        # driver.get(self.url)

        # click_keyi_username = driver.find_element_by_xpath("//div[@class='wp-onb-tit']/a[text()='可疑用户']")
        # self.webdriverwait_click(driver, click_keyi_username)

        self.driver.get(url)
        self.driver.implicitly_wait(5)
        res = self.driver.find_element_by_xpath("/html/body/iframe")
        # print(res.get_attribute("src"))
        # print(self.driver.page_source)
        #self.driver.get(res.get_attribute("src"))

        # login_button = driver.find_element_by_id('slide_bar_head')
        # self.webdriverwait_click(driver, login_button)
        # time.sleep(1)

        driver.switch_to.frame(driver.find_element_by_xpath('/html/body/iframe'))  # switch 到 滑块frame
        time.sleep(0.5)
        try:
            bk_block = driver.find_element_by_xpath('//*[@id="slideBkg"]')  # 大图
        except:
            bk_block = driver.find_element_by_xpath('//*[@id="container_body"]/div/div[1]/div/div[2]/img')
        web_image_width = bk_block.size
        web_image_width = web_image_width['width']
        bk_block_x = bk_block.location['x']

        try:
            slide_block = driver.find_element_by_xpath('//*[@id="slideBlock"]')  # 小滑块
        except:
            slide_block = driver.find_element_by_xpath('//*[@id="slideBlock"]')  # 小滑块
        slide_block_x = slide_block.location['x']
        bk_block = bk_block.get_attribute('src')       # 大图 url

        slide_block = slide_block.get_attribute('src')  # 小滑块 图片url
        slid_ing = driver.find_element_by_xpath('//div[@id="slide_bar_head"]')  # 滑块

        os.makedirs('./image/', exist_ok=True)
        self.urllib_download(bk_block, './image/bkBlock_%s.png' % self.timestamp)
        self.urllib_download(slide_block, './image/slideBlock_%s.png' % self.timestamp)
        time.sleep(0.5)
        img_bkblock = Image.open('./image/bkBlock_%s.png' % self.timestamp)
        real_width = img_bkblock.size[0]
        width_scale = float(real_width) / float(web_image_width)
        position = self.get_postion('./image/bkBlock_%s.png' % self.timestamp, './image/slideBlock_%s.png' % self.timestamp)
        real_position = position[1] / width_scale
        real_position = real_position - (slide_block_x - bk_block_x)
        track_list = self.get_track(real_position + 5)

        ActionChains(driver).click_and_hold(on_element=slid_ing).perform()  # 点击鼠标左键，按住不放
        time.sleep(0.1)
        # print('第二步,拖动元素')
        for track in track_list:
            ActionChains(driver).move_by_offset(xoffset=track, yoffset=random.randrange(-2,2)).perform()  # 鼠标移动到距离当前位置（x,y）
            # time.sleep(0.001)
        # ActionChains(driver).move_by_offset(xoffset=-random.randint(0, 1), yoffset=0).perform()   # 微调，根据实际情况微调
        time.sleep(1)
        # print('第三步,释放鼠标')
        ActionChains(driver).release(on_element=slid_ing).perform()
        print('登录成功')
        time.sleep(2)
        self.driver.get(url)
        self.driver.implicitly_wait(5)

    # @classmethod
    def get_cookies_and_front(self, url):
        retry_times = 0
        cookie_dic = {}
        rds_len = self.rds.llen("kwai_cookies")
        if rds_len <= 10:
            self.driver = webdriver.Chrome(options=self.chrome_options)
            while retry_times < 5:
                try:
                    self.login_main(url)
                    res = self.driver.find_element_by_xpath('//*[@id="slideBlock"]')
                    retry_times += 1
                    print("retry_times ",retry_times)
                except Exception as e:
                    print(e)
                    cookie = self.driver.get_cookies()
                    for k in cookie:
                        cookie_dic[k["name"]] = k["value"]
                    self.rds.lpush("kwai_cookies", json.dumps(cookie_dic))
                    # print("cookies:",cookie_dic)
                    self.after_quit()
                    break
        else:
            cookie_dic = json.loads(self.rds.lindex("kwai_cookies",random.randint(0,rds_len-1)))

        os_path = "/home/hanye/"
        page_html = requests.get(url,headers=self.first_page_headers,cookies=cookie_dic)
        # print(page_html.text)
        this_path = os.path.isdir(os_path)
        if not this_path:
            os_path = "."
        # font_face = self.driver.find_element_by_xpath("/html/head/style[1]")
        font_woff_link = re.findall("url\('(.*?)'\)\s+format\('woff'\)", page_html.text)
        if not font_woff_link:
            self.delete_cookies(cookie_dic)
            self.get_cookies_and_front(url)
        woff_name = font_woff_link[0].split("/")[-1]
        # print(woff_name)
        try:
            f = open("%s/%s.xml" % (os_path, woff_name), encoding="utf-8")
        except:
            woff = requests.get(font_woff_link[0],
                                headers={
                                        "Referer": url,
                                        "Sec-Fetch-Mode": "cors",
                                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36"}).content
            with open("%s/%s" % (os_path, woff_name), "wb") as f:
                f.write(woff)
            font = TTFont("%s/%s" % (os_path, woff_name))
            font.saveXML("%s/%s.xml" % (os_path, woff_name))
            f = open("%s/%s.xml" % (os_path, woff_name), encoding="utf-8")

        # f = open("./%s.xml" % woff_name, encoding="utf-8")
        self.xml_text = f.read()
        self.uni_code_dic = self.get_num_dic()
        self.del_file()
        return cookie_dic,self.uni_code_dic

    def delete_cookies(self,cookie_dic):
        # print(type(json.dumps(cookie_dic)))
        res = self.rds.lrem("kwai_cookies",1,json.dumps(cookie_dic))

    def del_file(self):
        file_path = ["./image/bkBlock_%s.png" % self.timestamp,"./image/slideBlock_%s.png" % self.timestamp,"./image/temp_%s.jpg" % self.timestamp,"./image/targ_%s.jpg" % self.timestamp]
        for single_path in file_path:
            try:
                os.remove(single_path)
            except:
                continue

# login = Login()

# if __name__ == '__main__':
#     login = Login()
#     cookie_dic,uni_code_dic = login.get_cookies_and_front("https://live.kuaishou.com/profile/3xw8s48b2q7htx9?csr=true")
