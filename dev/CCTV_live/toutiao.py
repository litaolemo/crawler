# -*- coding:utf-8 -*-
# @Time : 2020/5/28 15:19 
# @Author : litao
from selenium import webdriver
import redis, datetime,time,json
from crawler.crawler_sys.utils.trans_str_play_count_to_int import trans_play_count
rds = redis.StrictRedis(host='192.168.17.60', port=6379, db=15, decode_responses=True)

class CrawlerMain(object):
    def __init__(self):
        self.chrome_options = webdriver.ChromeOptions()
        # self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument("--start-maximized")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument('disable-infobars')
        # 请根据chrome版本 下载chrome driver   http://npm.taobao.org/mirrors/chromedriver/
        self.driver = webdriver.Chrome(options=self.chrome_options)

    def login(self):
        self.driver.get("https://live.ixigua.com/room/6831736034540456716/")
        while True:
            now = datetime.datetime.now()
            res = self.driver.find_elements_by_xpath("//span[@class='action-text v-middle live-skin-normal-text dp-i-block']")
            if res:
                play_count = trans_play_count(res[0].text)
                print(play_count)
                dic = {
                        "menber":play_count,
                        "fetch_time":int(now.timestamp()*1e3)
                }
                rds.rpush("toutiao", json.dumps(dic))
                time.sleep(200)

    def __exit__(self):
        self.driver.close()

if __name__ == "__main__":
    test = CrawlerMain()
    test.login()
