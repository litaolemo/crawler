from selenium import webdriver
import datetime
import re
import pandas as pd


class Craler_tudou(object):
    def __init__(self):
        chrome_options = webdriver.ChromeOptions()
        prefs = {"profile.managed_default_content_settings.images": 2}
        chrome_options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome(chrome_options=chrome_options)


    @staticmethod
    def video_time(time_str):
        now = datetime.datetime.now()
        if "分钟前" in time_str:
            min_str = re.findall(r"(\d+)分钟前",time_str)[0]
            videotime = now - datetime.timedelta(minutes=int(min_str))
        elif "小时前" in time_str:
            hour_str = re.findall(r"(\d+)小时前", time_str)[0]
            videotime = now - datetime.timedelta(hours=int(hour_str))
        elif "昨天" in time_str:
            date_lis = time_str.split(" ")
            hours, mins = date_lis[1].split(":")
            last_day = now - datetime.timedelta(days=1)
            videotime = datetime.datetime(year=int(last_day.year), month=int(last_day.month), day=int(last_day.day), hour=int(hours), minute=int(mins))
        elif "前天" in time_str:
            date_lis = time_str.split(" ")
            hours, mins = date_lis[1].split(":")
            last_day = now - datetime.timedelta(days=2)
            videotime = datetime.datetime(year=int(last_day.year), month=int(last_day.month), day=int(last_day.day), hour=int(hours), minute=int(mins))
        elif "天前" in time_str:
            day_str = re.findall(r"(\d+)天前", time_str)[0]
            videotime = now - datetime.timedelta(days=int(day_str))
        elif "刚刚" in time_str:
            videotime = now
        else:
            if str(now.year) in time_str:
                pass
            else:
                date_lis = time_str.split(" ")
                month,days = date_lis[0].split("-")
                hours,mins = date_lis[1].split(":")
                videotime = datetime.datetime(year=int(now.year),month=int(month),day=int(days),hour=int(hours),minute=int(mins))

        # print(videotime.strftime("%Y-%m-%d %H:%M:%S"))
        return videotime

    def time_range_video_num(self,start_time,end_time,url_list):
        data_lis = []
        info_lis = []
        columns = [""]
        for dic in url_list:
            for res in self.get_page(dic["url"]):
                title,link,video_time = res
                print(res)
                if start_time < video_time < end_time:
                    data_lis.append((title,link,video_time,dic["url"]))
                else:
                    break
            csv_save = pd.DataFrame(data_lis)
            csv_save.to_csv("%s.csv" % (dic["platform"] + "_" + dic["releaser"]),encoding="GBK")
            info_lis.append([dic["platform"],dic["releaser"],len(data_lis)])
            data_lis = []
        csv_save = pd.DataFrame(info_lis)
        csv_save.to_csv("%s.csv" % (datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")), encoding="GBK")
        self.driver.quit()

    def get_page(self,url):
        #video_page = self.driver.get(url)
        # js = 'window.open("%s");' % url
        # self.driver.execute_script(js)
        page_num = -1
        try:
            video_page = self.driver.get(url)
            while True:
                page_num += 1
                if page_num != 0:
                    self.driver.find_element_by_class_name("next").click()
                video_lis = self.driver.find_elements_by_xpath("/html/body/div[2]/div/div[3]/div/div/div/div[2]/div/div/div/div[1]/div")
                for v in video_lis:
                    v_a = v.find_element_by_xpath("./div[2]/a")
                    title = v_a.get_attribute("title")
                    link = v_a.get_attribute("href")
                    video_time = self.video_time(v.find_element_by_class_name("v-publishtime").text)

                    yield (title,link,video_time)

        except Exception as e:
            raise e
            print(e)
            print("page %s has no more data" % page_num)

if __name__ == "__main__":
    test = Craler_tudou()
    url_lis = [
        {"platform":"new_tudou",
        "url":"https://id.tudou.com/i/UNTk2NjE0MDM4NA==/videos?",
         "releaser":"酷娱文化先锋"
         },
        {"platform": "new_tudou",
         "url": "https://id.tudou.com/i/UMTQ3MDM0MjAw/videos?",
         "releaser": "酷娱文化先锋"
         }]
    start_time = datetime.datetime(year=2019,month=6,day=6)
    end = datetime.datetime.now()
    test.time_range_video_num(start_time,end,url_lis)