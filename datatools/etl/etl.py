# -*- coding: utf-8 -*-
import sys
import time
import logging

from .compat2to3 import basestring


logger = logging.getLogger(__name__)


class DataExtractor(object):
    def getrows(self, top=0):
        pass

    def close(self):
        pass


class DataTransformer(object):
    def transform(self, row):
        return row


class SimpleDataTransformer(DataTransformer):
    def __init__(self, config, handlers=None):
        self.config = config
        self.handlers = handlers

    @staticmethod
    def _strfix(val):
        if val and isinstance(val, basestring):
            val = ' '.join(val.split())
        return val

    def _process_handler(self, name, field, val):
        if self.handlers:
            handler = self.handlers.get(name)
            if handler:
                val = handler.handle(field, val)
        return val

    def get_val(self, row, field):
        val = None
        source = field.get('source')
        if source:
            val = row.get(source) or row.get(source.lower())
            handler_name = field.get('handler')
            if handler_name:
                val = self._process_handler(handler_name, field, val)

        if not val and 'default' in field:
            val = field.get('default')

        field_type = field.get('type')
        if field_type == 'str':
            val = val and str(val) or val
        elif field_type == 'date_str':
            fmt = field.get('format')
            val = val and val.strftime(fmt) or val
        
        return val
    
    def transform(self, row):
        vals = []
        fields = self.config.settings.transformer.mapping.fields
        for field in fields:
            val = self.get_val(row, field)
            name = field.get('name')
            vals.append((name, self._strfix(val)))
                           
        return vals
            

class DataLoader(object):
    def load(self, datacol, callback=None, **kwargs):
        pass

    def optimize(self):
        pass

    def close(self):
        pass


def console_callback(d):
    elapsed_time = time.time() - d['start_time']
    sys.stdout.write('processed:%d, elapsed time:%.3f\r'
                     % (d['current'], elapsed_time))


def etl(config, extractor, transformer, loader, callback=console_callback):
    try:
        start_time = time.time()

        def generate_data(extractor, transformer):
            for row in extractor.getrows():
                data = transformer.transform(row)
                if data is not None:
                    yield data
        
        total = loader.load(generate_data(extractor, transformer),
                            callback, start_time=start_time)        
        elapsed_time = time.time() - start_time
        logger.info('insert total:%d, execution time:%.3f' % (total, elapsed_time))
        
        # optional optimize
        loader.optimize()

        return start_time, total
    except Exception as e:
        logger.exception(hasattr(e, 'value') and e.value[1] or e)
        raise e
    finally:
        if extractor:
            extractor.close()
        if loader:
            loader.close()
