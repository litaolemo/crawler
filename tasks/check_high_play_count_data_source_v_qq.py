# -*- coding: utf-8 -*-
"""
Created on Wed Sep  5 17:52:53 2018

@author: fangyucheng
"""

from crawler_sys.site_crawler.crawler_v_qq import Crawler_v_qq
from crawler_sys.utils.output_results import output_result
from crawler_sys.utils import Metaorphosis as meta
from crawler_sys.utils.output_log import output_log

logging = output_log(page_category='video_page',
                     program_info='tencent')

def tran_input_data_to_lst(file_name, file_category='csv'):
    if file_category == 'csv':
        video_info_lst = meta.csv_to_lst_whth_headline(file_name)
        url_lst = []
        for line in video_info_lst:
            try:
                if line['data_provider'] == 'CCR':
                    url_lst.append(line['url'])
            except:
                pass
        return url_lst
    elif file_category == 'file':
        url_lst = meta.str_file_to_lst(file_name)
        return url_lst

url_lst = tran_input_data_to_lst(file_name='R:/CCR/数据需求/短期临时需求/TX', file_category='file')

crawler = Crawler_v_qq()
get_video_page = crawler.video_page

def get_data_source(url_lst=url_lst,
                    output_to_file=False,
                    filepath=None,
                    output_to_es_raw=False,
                    output_to_es_register=False,
                    push_to_redis=False,
                    output_es_index=None,
                    output_doc_type=None):
    result_lst = []
    for url in url_lst:
        video_info = get_video_page(url=url)
        result_lst.append(video_info)
        logging.info('get_data at page %s' % url)
        if len(result_lst) >= 100:
            if output_es_index is not None and output_doc_type is not None:
                output_result(result_lst,
                              platform='腾讯视频',
                              output_to_file=output_to_file,
                              output_to_es_raw=output_to_es_raw,
                              output_to_es_register=output_to_es_register,
                              push_to_redis=push_to_redis,
                              es_index=output_es_index,
                              doc_type=output_doc_type)
                result_lst.clear()
            else:
                output_result(result_lst,
                              platform='腾讯视频',
                              output_to_file=output_to_file,
                              output_to_es_raw=output_to_es_raw,
                              output_to_es_register=output_to_es_register,
                              push_to_redis=push_to_redis)
                result_lst.clear()
    if len(result_lst) != []:
        if output_es_index is not None and output_doc_type is not None:
            output_result(result_lst,
                          platform='腾讯视频',
                          output_to_file=output_to_file,
                          output_to_es_raw=output_to_es_raw,
                          output_to_es_register=output_to_es_register,
                          push_to_redis=push_to_redis,
                          es_index=output_es_index,
                          doc_type=output_doc_type)
            result_lst.clear()
        else:
            output_result(result_lst,
                          platform='腾讯视频',
                          output_to_file=output_to_file,
                          output_to_es_raw=output_to_es_raw,
                          output_to_es_register=output_to_es_register,
                          push_to_redis=push_to_redis)
            result_lst.clear()

if __name__ == '__main__':
    get_data_source(output_to_es_raw=True,
                    output_es_index='test2',
                    output_doc_type='fyc')