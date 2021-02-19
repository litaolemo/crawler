# -*- coding:utf-8 -*-
# @Time : 2019/8/6 10:47 
# @Author : litao
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sys,os

class WeiboLogin():
    def __init__(self, username, password):
        # os.system('pkill -f phantom')
        chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument('--headless')
        # chrome_options.add_argument('--disable-gpu')
        #      driver = webdriver.Remote(command_executor='http://192.168.18.11:4444/wd/hub',
        # desired_capabilities=DesiredCapabilities.CHROME)
        self.url = 'https://passport.weibo.cn/signin/login?entry=mweibo&r=https://weibo.cn/'
        self.browser = webdriver.Chrome(r'../chromedriver.exe', options=chrome_options)
        #self.browser.set_window_size(1050, 840)
        self.wait = WebDriverWait(self.browser, 20)
        self.username = username
        self.password = password

    def open(self):
        """
        打开网页输入用户名密码并点击
        :return: None
        """
        self.browser.get(self.url)
        username = self.wait.until(EC.presence_of_element_located((By.ID, 'loginName')))
        password = self.wait.until(EC.presence_of_element_located((By.ID, 'loginPassword')))
        submit = self.wait.until(EC.element_to_be_clickable((By.ID, 'loginAction')))
        username.send_keys(self.username)
        password.send_keys(self.password)
        submit.click()

    def run(self):
        """
        破解入口
        :return:
        """
        self.open()
        WebDriverWait(self.browser, 30).until(
            EC.title_is('我的首页')
        )
        cookies = self.browser.get_cookies()
        cookie = [item["name"] + "=" + item["value"] for item in cookies]
        cookie_str = '; '.join(item for item in cookie)
        self.browser.quit()
        return str(cookie_str)

if __name__ == '__main__':
    user_name = '13910233534'
    password = 'Lemo1995'
    cookie_str = WeiboLogin(user_name, password).run()
    print(cookie_str,type(cookie_str))
    with open("./cookie_pool","a",encoding="utf-8") as f:
        f.write(cookie_str+"\n")
