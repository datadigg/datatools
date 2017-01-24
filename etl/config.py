# -*- coding: utf-8 -*-
import os, yaml
import logging, logging.config

BASE_DIR = os.path.dirname(__file__)
SETTINGS_FILE = 'settings.yml'

loggingfilepath = os.path.join(BASE_DIR,'logging.yml')
logging.config.dictConfig(yaml.load(open(loggingfilepath,'r')))

class ConfigObjectWrapper(object):
    def __init__(self, data):
        for name, value in data.iteritems():
            setattr(self, name, self._wrap(value))

    def _wrap(self, value):
        if isinstance(value, (tuple, list, set, frozenset)): 
            return type(value)([self._wrap(v) for v in value])
        else:
            return ConfigObjectWrapper(value) \
                   if isinstance(value, dict) else value
        
class Configuration(object):
    def __init__(self, argv):
        self.argv = argv
        self.conf_dir = argv[0]
        self.settings = self.getconfig(SETTINGS_FILE)

    def getconfig(self, fn):
        filepath = os.path.join(self.conf_dir,fn)
        if os.path.exists(filepath):
            logging.debug('loading:%s' % filepath)
            datadict = yaml.safe_load(open(filepath,'r'))
            return ConfigObjectWrapper(datadict)
