#-*- coding:utf-8 -*-
#!/usr/bin/env python

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

sys.path.append('../../')
import os
import logging
import logging.handlers
import Config

LEVELS = {'NOSET': logging.NOTSET,
          'DEBUG': logging.DEBUG,
          'INFO': logging.INFO,
          'WARNING': logging.WARNING,
          'ERROR': logging.ERROR,
          'CRITICAL': logging.CRITICAL}

global logger
logger = None

# create logs file folder
def config_logging(logger_name="crawler", file_name="crawler", log_level="INFO"):
    '''
    @summary: config logging to write logs to local file
    @param logger_name
    @param file_name: name of log file
    @param log_level: log level
    '''
    logs_dir = os.path.join(Config.logPath, logger_name)
    if os.path.exists(logs_dir) and os.path.isdir(logs_dir):
        pass
    else:
        os.makedirs(logs_dir)

    # clear old root logger handlers
    #logging.getLogger("").handlers = []

    CUR_PID = os.getpid()
    file_name = os.path.join(logs_dir, file_name+"_"+str(CUR_PID)+".log")

    global logger
    # set initial log level
    logger = logging.getLogger(logger_name)
    level = LEVELS[log_level.upper()]
    logger.setLevel(level)

    # define a rotating file handler
    rotatingFileHandler = logging.handlers.RotatingFileHandler(filename=file_name,
                                                      maxBytes = 1024 * 1024 * 50,
                                                      backupCount = 5)
    #formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    formatter = logging.Formatter("[%(asctime)s %(name)s %(levelname)s %(filename)s/%(module)s/%(funcName)s/%(lineno)d] %(message)s")
    rotatingFileHandler.setFormatter(formatter)
    logger.addHandler(rotatingFileHandler)
    
    # define a handler whitch writes messages to sys
    #console = logging.StreamHandler()
    # set a format which is simple for console use
    #formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
    # tell the handler to use this format
    #console.setFormatter(formatter)
    # add the handler to the root logger
    #logger.addHandler(console)

