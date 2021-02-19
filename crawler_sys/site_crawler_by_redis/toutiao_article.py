# -*- coding:utf-8 -*-
# @Time : 2020/5/15 15:11 
# @Author : litao
from crawler.crawler_sys.site_crawler_by_redis.crawler_toutiao import Crawler_toutiao


class Crawler_toutiao_article(Crawler_toutiao):
    def __init__(self):
        super().__init__()

    def releaser_page_by_time(self, start_time=None, end_time=None, url=None, allow=None, **kwargs):
        count_false = 0
        for res in self.article_page(url, proxies_num=kwargs.get("proxies_num")):
            video_time = res["release_time"]
            # print(res)
            if video_time:
                if start_time < video_time:
                    if video_time < end_time:
                        yield res
                else:
                    count_false += 1
                    if count_false > allow:
                        break
                    else:
                        yield res
        count_false = 0
        for res in self.microheadlines_page(url, proxies_num=kwargs.get("proxies_num")):
            video_time = res["release_time"]
            print(video_time)
            if video_time:
                if start_time < video_time:
                    if video_time < end_time:
                        yield res
                else:
                    count_false += 1
                    if count_false > allow:
                        break
                    else:
                        yield res

if __name__ == "__main__":
    test = Crawler_toutiao_article()
    for a in test.article_page("https://www.toutiao.com/c/user/5821222208/#mid=5821222208"):
        print(a)