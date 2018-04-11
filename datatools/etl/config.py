# -*- coding: utf-8 -*-
import os, yaml, json
import logging

from yconf.util import NestedDict

logger = logging.getLogger(__name__)


def check_config(obj, name):
    return name in obj \
        if isinstance(obj, dict) else hasattr(obj, name)


def get_config(obj, name):
    return obj.get(name, None) \
        if isinstance(obj, dict) else getattr(obj, name, None)


class Configuration(object):
    def __init__(self, d):
        self.args = NestedDict(d)
        self.settings = self._get_config(self.args, 'settings.yml')
        d = json.loads(self.args.settings)
        self.settings.update(d)

    @staticmethod
    def _get_config(args, fn):
        d = {}
        file_path = os.path.join(args.conf, fn)
        if os.path.exists(file_path):
            logger.debug('loading:%s' % file_path)
            docs = yaml.load_all(open(file_path,'r'))
            profile = args.profile or 'default'
            d = next(doc for doc in docs if doc.get('profile') == profile)
            
        return NestedDict(d)
