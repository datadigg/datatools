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
        self.commit_size = self.get_attr(config.settings.loader,
                                         'autoCommitSize') or 1000

    def _get_insert_sql(self, cols, table):
        return '''insert into %s(%s)values(%s)'''\
                  % (table, ','.join(cols),','.join('?'*len(cols)))
        
    def load(self, datacol, callback=None, **kwargs):
        cols,vals = zip(*datacol[0])
        insertsql = self._get_insert_sql(cols, self.table)

        total = 0
        vals_list = []
        for data in datacol:
            total += 1
            if callback:
                callback({'start_time': kwargs['start_time'],
                          'current': total})
            vals_list.append(zip(*data)[1])
            if len(vals_list) == self.commit_size:
                self.cursor.executemany(insertsql, vals_list)
                self.conn.commit()
                vals_list[:] = []
        if vals_list:
            self.cursor.executemany(insertsql, vals_list)
            self.conn.commit()
            
        return total
        
    def close(self):
        self.conn.close()
