# -*- coding: utf-8 -*-
import os, yaml, json
import logging

from yconf.util import NestedDict
        
class Configuration(object):
    def __init__(self, d):
        self.args = NestedDict(d)
        self.settings = self._getconfig(self.args, 'settings.yml')
        d = json.loads(self.args.settings)
        self.settings.update(d)

    def _getconfig(self, args, fn):
        d = {}
        filepath = os.path.join(args.conf,fn)
        if os.path.exists(filepath):
            logging.debug('loading:%s' % filepath)
            docs = yaml.load_all(open(filepath,'r'))
            profile = args.profile or 'default'
            d = next(doc for doc in docs \
                  if doc.get('profile') == profile)
            
        return NestedDict(d)
