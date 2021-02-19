# -*- coding: utf-8 -*-
"""
Created on Wed Jun 13 10:52:58 2018

tested on toutiao

@author: hanye
"""

import re
from crawler_sys.utils.output_results import retry_get_url

def get_redirected_resp(ori_url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,en-US;q=0.7,en;q=0.3",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
            }
    get_host_str = re.findall('://.+?/', ori_url)
    if get_host_str!=[]:
        host_str = get_host_str[0].replace(':', '').replace('/', '')
        headers['Host'] = host_str
    else:
        pass

    # must use allow_redirects=False to avoiding automatically redirect by requests package
    r_1st = retry_get_url(ori_url, headers=headers, allow_redirects=False)
    print('Respond from direct get: %s' % r_1st)
    if r_1st!=None:
        if 'location' in r_1st.headers:
            print('location in response headers: %s' % r_1st.headers['location'])
            redirect_url = r_1st.headers['location']
        else:
            print('There is no location field in response headers, '
                  'will check the response.history attribute.')
            if len(r_1st.history)>0:
                history_headers = r_1st.history[0].headers
                if 'Location' in history_headers:
                    redirect_url = history_headers['Location']
                    print('Original url %s redirected to %s' % (ori_url, redirect_url))
                    print('response history: %s\n' % r_1st.history)
                else:
                    redirect_url = None
            else:
                print('No further redirects.')
                redirect_url = ori_url
                return r_1st

        if redirect_url!=None and redirect_url!=ori_url:
            redirect_url = get_redirected_resp(redirect_url)

        return redirect_url
    else:
        return None

