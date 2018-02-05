# -*- coding: utf-8 -*-
import os, logging
import pyodbc, pypyodbc
from etl import DataExtractor,DataTransformer,DataLoader

logger = logging.getLogger(__name__)

class ODBCDataExtractor(DataExtractor):
    def __init__(self, config):
        odbc = config.settings.extractor.odbc
        connstr = config.args.dbfile \
                  and odbc.connstr % config.args.dbfile or odbc.connstr 
        logger.debug('ODBCDataExtractor connstr:%s' % connstr)
        self.conn = pypyodbc.connect(connstr)
        self.cursor = self.conn.cursor()
        if odbc.table:
            self.table = odbc.table
        else:
            tables = [tb.table_name for tb in self.cursor.tables() \
                      if tb.table_type == 'TABLE']
            logger.debug('tables:%s' % tables);
            self.table = tables[0]

    def getrows(self, top=0):
        topstr = ''
        if top > 0:
            topstr = 'top %s'%top
        sql = '''select %s * from [%s]'''\
                % (topstr,self.table,)
        logger.debug('execute sql:[%s]' % sql)
        self.cursor.execute(sql)
        cols = [col[0].lower() for col in self.cursor.description]
        logger.debug('extract columns:%s' % cols)
        for row in self.cursor:
            yield dict(zip(cols,row))
        
    def close(self):
        self.conn.close()
        

class ODBCDataLoader(DataLoader):
    def __init__(self, config):
        odbc = config.settings.loader.odbc
        logger.debug('ODBCDataLoader init:')
        logger.debug('connstr:%s' % odbc.connstr)
        logger.debug('table:%s' % odbc.table)
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
