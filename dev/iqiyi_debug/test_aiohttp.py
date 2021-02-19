# -*- coding: utf-8 -*-
"""
Created on Fri Dec  7 11:02:17 2018

@author: fangyucheng
"""

import aiohttp
import asyncio

task_list = ['http://list.iqiyi.com/www/10/1007-------------4-8-2--1-.html',
             'http://list.iqiyi.com/www/24/-------------4-27-2-iqiyi--.html',
             'http://list.iqiyi.com/www/28/-------------4-12-2-iqiyi-1-.html',
             'http://list.iqiyi.com/www/17/-------------4-11-2-iqiyi--.html',]




async def download_page(session, url):
    get_page = await session.get(url)
    page = await get_page.text("utf-8", errors="ignore")
    return page

async def get_list_page(loop):
    async with aiohttp.ClientSession() as list_page_sess:
        task = [loop.create_task(download_page(list_page_sess, url)) for url in task_list]
        done, pending = await asyncio.wait(task)
        result_lst = [d.result() for d in done]
        print(result_lst)

loop = asyncio.get_event_loop()
loop.run_until_complete(get_list_page(loop))