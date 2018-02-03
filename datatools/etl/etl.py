# -*- coding: utf-8 -*-
import os, sys, time, logging

class CommonBase(object):

    def check_attr(self, obj, name):
        return name in obj \
               if isinstance(obj, dict) else hasattr(obj, name)
    
    def get_attr(self, obj, name):
        return obj.get(name) \
               if isinstance(obj, dict) else getattr(obj, name, None)

class DataExtractor(CommonBase):
    def getrows(self, top=0):
        pass

    def close(self):
        pass

class DataTransformer(CommonBase):
    def transform(self, row):
        return row

class SimpleDataTransformer(DataTransformer):
    def __init__(self, config):
        self.config = config

    def _strfix(self, val):
        if val:
            if isinstance(val, basestring):
                val = ' '.join(val.split())
        return val

    def get_val(self, row, field):
        val = None
        source = self.get_attr(field, 'source')
        if source:
            val = row.get(source) or row.get(source.lower())
        if not val and self.check_attr(field, 'default'):
            val = self.get_attr(field, 'default')

        field_type = self.get_attr(field, 'type')
        if field_type == 'str':
            val = val and str(val) or val
        elif field_type == 'date_str':
            fmt = self.get_attr(field, 'format')
            val = val and val.strftime(fmt) or val
        
        return val
    
    def transform(self, row):
        vals = []
        fields = self.config.settings.transformer.mapping.fields
        for field in fields:
            val = self.get_val(row, field)
            name = self.get_attr(field, 'name')
            vals.append((name, self._strfix(val)))
                           
        return vals
            

class DataLoader(CommonBase):
    def load(self, datacol):
        pass

    def optimize(self):
        pass

    def close(self):
        pass

def console_callback(start_time, current):
    elapsed_time = time.time() - start_time
    sys.stdout.write('processed:%d, elapsed time:%.3f\r'
                     % (current, elapsed_time))
        
def etl(config, extractor, transformer, loader, callback=console_callback):
    try:
        start_time = time.time()
        total = [0]
        commit_size = config.settings.loader.autoCommitSize
        def _callback_data(current):
            return {'start_time': start_time, 'current': current}
        if commit_size > 0:
            datacol = []
            for row in extractor.getrows():
                total[0] += 1
                if callback:
                    callback(**_callback_data(total[0]))
                data = transformer.transform(row)
                if data:
                    datacol.append(data)
                if len(datacol) == commit_size:
                    loader.load(datacol)
                    datacol[:] = []
            if datacol:
                loader.load(datacol)
        else:
            def generate_data(extractor, transformer):
                for row in extractor.getrows():
                    total[0] += 1
                    if callback:
                        callback(**_callback_data(total[0]))
                    yield transformer.transform(row)
                    
            loader.load(generate_data(extractor, transformer))
                   
        elapsed_time = time.time() - start_time
        logging.debug('insert total:%d, execution time:%.3f' \
              % (total[0], elapsed_time))
        
        # optional optimize
        loader.optimize()

        return total[0]
    except Exception as e:
        logging.exception(hasattr(e,'value') and e.value[1] or e)
        raise e
    finally:
        if extractor: extractor.close()
        if loader: loader.close()
