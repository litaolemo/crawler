# -*- coding: utf-8 -*-
"""
Created on Wed Nov 21 09:08:29 2018

@author: fangyucheng
"""

import datetime

def calculator(shifting_days=30,
               shifting_hours=0,
               shifting_minutes=0):
    now = datetime.datetime.now()
    if shifting_hours == 0 and shifting_minutes == 0 and shifting_days != 0:
        date_shift = now - datetime.timedelta(days=shifting_days)
        date_shift_str = str(date_shift)[:10]
        date_wanted = datetime.datetime.strptime(date_shift_str,
                                                 "%Y-%m-%d").timestamp() * 1e3
        return int(date_wanted)