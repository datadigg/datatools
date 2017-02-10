# -*- coding: utf-8 -*-
import os, logging
import pyodbc, pypyodbc
from etl import DataExtractor,DataTransformer,DataLoader

class ODBCDataExtractor(DataExtractor):
    def __init__(self, config):
        odbc = config.settings.extractor.odbc
        logging.debug('ODBCDataExtractor connstr:%s' % odbc.connstr)
        self.conn = pyodbc.connect(odbc.connstr)
        self.cursor = self.conn.cursor()
        if odbc.table:
            self.table = odbc.table
        else:
            tables = [tb.table_name for tb in self.cursor.tables() \
                      if tb.table_type == 'TABLE']
            logging.debug('tables:%s' % tables);
            self.table = tables[0]

    def getrows(self, top=0):
        topstr = ''
        if top > 0:
            topstr = 'top %s'%top
        sql = '''select %s * from [%s]'''\
                % (topstr,self.table,)
        logging.debug('execute sql:[%s]' % sql)
        self.cursor.execute(sql)
        cols = [col[0].lower() for col in self.cursor.description]
        logging.debug('extract columns:%s' % cols)
        for row in self.cursor:
            yield dict(zip(cols,row))
        
    def close(self):
        self.conn.close()

class ODBCDataTransformer(DataTransformer):
    def __init__(self, config):
        self.config = config
    
    def transform(self, row):
        vals = []
        fields = self.config.settings.transformer.mapping.fields
        for field in fields:
            val = row.get(field.source)
            vals.append((field.name, val))
                           
        return vals
        

class ODBCDataLoader(DataLoader):
    def __init__(self, config):
        odbc = config.settings.loader.odbc
        logging.debug('ODBCDataLoader init:')
        logging.debug('connstr:%s' % odbc.connstr)
        logging.debug('table:%s' % odbc.table)
        self.conn = pypyodbc.connect(odbc.connstr)
        self.cursor = self.conn.cursor()
        self.table = odbc.table

    def _get_insert_sql(self, cols, table):
        return '''insert into %s(%s)values(%s)'''\
                  % (table, ','.join(cols),','.join('?'*len(cols)))
        
    def load(self, datacol):
        cols,vals = zip(*datacol[0])
        insertsql = self._get_insert_sql(cols, self.table)
        
        size = len(datacol)
        if size > 1:
            vals_list = []
            for data in datacol:
                vals_list.append(zip(*data)[1])
            self.cursor.executemany(insertsql, vals_list)
        else:
            self.cursor.execute(insertsql, vals)
        self.conn.commit()
        
    def close(self):
        self.conn.close()