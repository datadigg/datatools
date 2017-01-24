# -*- coding: utf-8 -*-
import os, sys, time, logging

class DataExtractor(object):
    def getrows(self, top=0):
        pass

    def close(self):
        pass

class DataTransformer(object):
    def transform(self, row):
        return row

class DataLoader(object):     
    def load(self, datalist):
        pass

def etl(config, extractor, transformer, loader):
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
