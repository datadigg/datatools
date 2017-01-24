# -*- coding: utf-8 -*-
import os, logging, errno

def mkdir_p(path):
    """http://stackoverflow.com/a/600612/190597 (tzot)"""
    try:
        os.makedirs(path, exist_ok=True)  # Python>3.2
    except TypeError:
        try:
            os.makedirs(path)
        except OSError as exc: # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else: raise

class TimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    def __init__(self, filename, when='h', interval=1, \
                 backupCount=0, encoding=None, delay=False, utc=False):            
        mkdir_p(os.path.dirname(filename))
        super(TimedRotatingFileHandler,self).__init__(\
            filename,when,interval,backupCount,encoding,delay,utc)


