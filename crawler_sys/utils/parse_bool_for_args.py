# -*- coding: utf-8 -*-
"""
Created on Fri Jun 29 12:46:19 2018

@author: hanye
"""

def parse_bool_for_args(arg_str):
    if arg_str.lower() in ('true', 'yes', 'y', '1'):
        return True
    elif arg_str.lower() in ('false', 'no', 'n', '0'):
        return False
    else:
        print('Illegal input! Bool like string values are needed.')
        return None
