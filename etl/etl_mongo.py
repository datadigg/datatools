# -*- coding: utf-8 -*-
import os, logging, datetime
from pymongo import MongoClient
from etl import DataExtractor,DataLoader,DataTransformer

class MongoDataExtractor(DataExtractor):
    def __init__(self, config):
        mongodb = config.settings.extractor.mongodb
        logging.debug('*' * 30)
        logging.debug('MongoDataExtractor init:')
        logging.debug('host:%s, port:%s' % (mongodb.host, mongodb.port))
        logging.debug('db:%s' % mongodb.db)
        logging.debug('collection:%s' % mongodb.collection)
        
        self.config = config
        self.mongodb = mongodb
        self.client = MongoClient(mongodb.host, mongodb.port)
        self.db = self.client[mongodb.db]
        if mongodb.username:
            self.db.authenticate(mongodb.username, mongodb.password)
        self.collection = self.db[mongodb.collection]

    def getrows(self, top=0):
        query =  eval(self.config.args.query)
        logging.debug('execute query:%s' % query)
        projection = eval(self.mongodb.projection)
        return self.collection.find(query, projection)
    
    def close(self):
        self.client.close()

class MongoDataTransformer(DataTransformer):
    def __init__(self, config):
        self.config = config

    def _clean_digit(self, val):
        return ''.join(c for c in val if c.isdigit())
    
    def _clean_str(self, val):
        return ''.join((e.isalnum() and e or '') for e in val).upper()
    
    def _transform(self, fields, row):
        query = {}
        update = {}
        for field in fields:
            val = row.get(field.source)
            indexed = getattr(field, "indexed", False)
            clean = getattr(field, "clean", False)
            isdigit = getattr(field, "isdigit", False)
            if val:
                if isinstance(val, list):
                    val = ' '.join(val)
                if isdigit:
                    val = self._clean_digit(val)
                if indexed:
                    query = { field.name : val }    
                ops = update.get(field.op)
                if ops:
                    ops.update({ field.name : val })
                else:
                    update[field.op] = { field.name : val }
                    
                if clean:
                    update.get(field.op).update(\
                        { '__' + field.name : self._clean_str(val) })
                           
        if query:
            return query,update
    
    def transform(self, row):
        mapping = self.config.settings.transformer.mapping
        if isinstance(mapping, list):
            data = {}
            for conf in mapping:
                result = self._transform(conf.fields, row)
                if result:
                    data[conf.tag] = result
        else:
            data = self._transform(mapping.fields, row)
        return data

class MongoDataLoader(DataLoader):
    def __init__(self, config):
        mongodb = config.settings.loader.mongodb
        logging.debug('*' * 30)
        logging.debug('MongoDataLoader init:')
        logging.debug('host:%s, port:%s' % (mongodb.host, mongodb.port))

        self.client = MongoClient(mongodb.host, mongodb.port)
        if hasattr(mongodb, 'dbs'):
            self.collections = {}
            for conf in mongodb.dbs:
                logging.debug('-' * 30)
                logging.debug('tag:%s' % conf.tag)
                collection = self._get_collection(conf)
                self.collections[conf.tag] = collection
        else:
            self.collection = self._get_collection(mongodb)

    def _get_collection(self, conf):
        logging.debug('db:%s' % conf.db)
        logging.debug('collection:%s' % conf.collection)
        db = self.client[conf.db]
        if hasattr(conf, 'username') and conf.username:
            db.authenticate(conf.username, conf.password)
        collection = db[conf.collection]
        collection.ensure_index(conf.indexKey, unique=True)
        return collection
                
    def load(self, datacol):
        for data in datacol:
            if isinstance(data, dict):
                for key, (query, update) in data.iteritems():
                    self.collections[key].update(query, update, upsert=True)
            else:
                query, update = data
                self.collection.update(query, update, upsert=True)

    def close(self):
        self.client.close()
        
        
