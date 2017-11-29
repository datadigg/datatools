# -*- coding: utf-8 -*-
import os, yaml
import logging, logging.config

from yconf.util import NestedDict

BASE_DIR = os.path.dirname(__file__)

loggingfilepath = os.path.join(BASE_DIR,'logging.yml')
logging.config.dictConfig(yaml.load(open(loggingfilepath,'r')))
        
class Configuration(object):
    def __init__(self, args):
        self.args = args
        self.settings = self._getconfig(args, 'settings.yml')

    def _getconfig(self, args, fn):
        filepath = os.path.join(args.conf,fn)
        if os.path.exists(filepath):
            logging.debug('loading:%s' % filepath)
            docs = yaml.load_all(open(filepath,'r'))
            profile = args.profile or 'default'
            doc = next(doc for doc in docs \
                  if doc.get('profile') == profile)
            
            return NestedDict(doc)
