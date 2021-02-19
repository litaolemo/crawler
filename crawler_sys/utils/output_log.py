# -*- coding: utf-8 -*-
"""
Created on Wed Dec 19 12:54:20 2018

@author: fangyucheng
"""

import logging
formatter = logging.Formatter('%(asctime)s %(name)s %(filename)s '
                              '%(funcName)s %(levelname)s %(message)s')

def init_logger(name, log_file, level=logging.INFO):
    """initialize logger"""
    #output log to file
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    #output log to screen
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    #initialize logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    #add handler and console to logger
    logger.addHandler(handler)
    logger.addHandler(console)
    return logger