# -*- coding: utf-8 -*-
import os, logging, datetime
from pymongo import MongoClient
from bson.son import SON
from etl import DataExtractor,DataLoader,DataTransformer

class MongoDataExtractor(DataExtractor):
    def __init__(self, config):
        mongodb = config.settings.extractor.mongodb
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

class MongoDataTransformer(DataTransformer):
    def __init__(self, config):
        self.config = config

    def _clean_digit(self, val):
        return ''.join(c for c in val if c.isdigit())
    
    def _clean_str(self, val):
        return ''.join((e.isalnum() and e or '') for e in val).upper()
    
    def transform(self, row):
        query = {}
        update = {}
        fields = self.config.settings.transformer.mapping.fields
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
                        { '_' + field.name : self._clean_str(val) })
                           
        if query:
            return query,update

class MongoDataLoader(DataLoader):
    def __init__(self, config):
        mongodb = config.settings.loader.mongodb
        logging.debug('MongoDataLoader init:')
        logging.debug('host:%s, port:%s' % (mongodb.host, mongodb.port))
        logging.debug('db:%s' % mongodb.db)
        logging.debug('collection:%s' % mongodb.collection)

        self.mongodb = mongodb
        self.client = MongoClient(mongodb.host, mongodb.port)
        self.db = self.client[mongodb.db]
        if mongodb.username:
            self.db.authenticate(mongodb.username, mongodb.password)
        self.collection = self.db[mongodb.collection]
        self.collection.ensure_index(mongodb.indexKey, unique=True)

    def load(self, datacol):
        indexKey = self.mongodb.indexKey
        for query,update in datacol:
            self.collection.update(query, update, upsert=True)

    def close(self):
        self.client.close()
        
        
