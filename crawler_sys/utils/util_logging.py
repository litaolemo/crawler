#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 20 10:07:00 2019

@author: hanye
"""
import datetime
import logging
import logging.handlers
from functools import wraps


def logged(func):
    """
    Decorator to log crawler task.
    """
    @wraps(func)
    def with_logging(*args, **kwargs):
        today_str = datetime.datetime.now().isoformat()[:10]
        log_folder_name = 'crawler_log'
        crawler_pth = '/home/hanye/crawlersNew/crawler'
        LOG_FN = ('crawler_task_%s_%s_log' % (func.__name__, today_str))
        log_fn_abs_path = '/'.join([crawler_pth, log_folder_name, LOG_FN])
        FORMAT = '[%(asctime)s][runningFunction:%(name)s][logModule:%(module)s][pid:%(process)d] %(message)s'
        hy_logger = logging.getLogger(func.__name__)
        hy_logger.setLevel(logging.INFO)
        formatter = logging.Formatter(fmt=FORMAT)
        file_handler = logging.FileHandler(filename=log_fn_abs_path)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        hy_logger.addHandler(file_handler)

        hy_logger.info('task starts')
        argstr = ''
        if args:
            argstr += args.__str__()
        if kwargs:
            argstr += kwargs.__str__()
        if argstr:
            hy_logger.info('args:%s' % argstr)

        return func(*args, **kwargs)
    return with_logging


