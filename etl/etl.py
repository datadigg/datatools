# -*- coding: utf-8 -*-
import os, sys, time, logging

class DataExtractor(object):
    def getrows(self, top=0):
        pass

    def close(self):
        pass

class DataTransformer(object):
    def __init__(self, config):
        self.config = config

    def _strfix(self, val):
        if val:
            if isinstance(val, basestring):
                val = ' '.join(val.split())
        return val
    
    def transform(self, row):
        vals = []
        fields = self.config.settings.transformer.mapping.fields
        for field in fields:
            val = row.get(field.source) or row.get(field.source.lower())
            vals.append((field.name, self._strfix(val)))
                           
        return vals

class DataLoader(object):     
    def load(self, datalist):
        pass

def etl(config, extractor, transformer, loader):
    try:
        start_time = time.time()
        total = 0
        datacol = []
        for row in extractor.getrows():
            data = transformer.transform(row)
    
            total += 1
            sys.stdout.write('processed:%s\r' % total)
            if data:
                datacol.append(data)
            if len(datacol) == config.settings.loader.autoCommitSize:
                loader.load(datacol)
                datacol[:] = []
    
        if datacol:
            loader.load(datacol)
        elapsed_time = time.time() - start_time
        logging.debug('insert total:%d, execution time:%.3f' \
              % (total, elapsed_time))
    except Exception as e:
        logging.exception(hasattr(e,'value') and e.value[1] or e)
        raise e
    finally:
        if extractor: extractor.close()
        if loader: loader.close()
