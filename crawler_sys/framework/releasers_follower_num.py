# -*- coding: utf-8 -*-
"""
Created on Thu Sep 27 19:06:38 2018

input platform and releaserUrl, output follower number to file

@author: fangyucheng
"""

import argparse
#from multiprocessing import Pool
from crawler.crawler_sys.utils import trans_format
from crawler.crawler_sys.framework.platform_crawler_register import get_crawler

parser = argparse.ArgumentParser(description='get releaser follow number')
parser.add_argument('-i', '--input', default='/home/hanye/crawlersNew/crawler/tasks/follower_num.csv',
                    help=('absolute path of input csv'))
parser.add_argument('-o', '--output', default='/home/hanye/crawlersNew/crawler/tasks/follower_num_result.csv',
                    help=('absolute path of output csv'))
parser.add_argument('-p', '--process_num', default=10, help=('process num'))
args = parser.parse_args()

input_file = args.input
output_file = args.output
input_file = r"D:\work_file\4月补数据1.csv"
tast_list = trans_format.csv_to_lst_with_headline(input_file)

for line in tast_list:
    releaserUrl = line['releaserUrl']
    platform = line['platform']
    follower_num = line['follower_num']
    if not follower_num:
        crawler_initialization = get_crawler(platform)
        try:
            crawler = crawler_initialization().get_releaser_follower_num
            line['follower_num'] = crawler(releaserUrl)
        except:
            line['follower_num'] = ""

output_file = "./follower_num_result1.csv"
trans_format.lst_to_csv(listname=tast_list, csvname=output_file)