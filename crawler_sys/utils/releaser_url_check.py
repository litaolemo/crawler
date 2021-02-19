# -*- coding: utf-8 -*-
"""
Created on Mon Aug  6 09:42:41 2018

@author: fangyucheng
"""


from crawler_sys.framework import platform_crawler_register


def test_releaserUrl(test_lst):
    for line in test_lst:
        try:
            platform = line['platform']
            platform_crawler = platform_crawler_register.get_crawler(platform)
            releaserUrl = line['releaserUrl']
            try:
                platform_crawler().releaser_page(releaserUrl=releaserUrl, 
                                                 releaser_page_num_max=1)
                line['True_or_False'] = 1
                line['add_mess'] = 'correct'
                print('get releaser page')
                print(line)
                yield line
            except:
                line['True_or_False'] = 0
                line['add_mess'] = 'wrong_url'
                print('%s can not get vaild info' % releaserUrl)
        except:
            pass
