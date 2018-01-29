# -*- coding: utf-8 -*-
import os, logging, datetime
from pymongo import MongoClient
from etl import DataExtractor,DataLoader,DataTransformer
from bson.son import SON

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
            self.db.authenticate(mongodb.username, mongodb.password, \
                                 source=mongodb.authenticationDatabase)
        self.collection = self.db[mongodb.collection]

    def getrows(self, top=0):
        query =  eval(self.config.args.query)
        logging.debug('execute query:%s' % query)
        projection = eval(self.mongodb.projection)
        return self.collection.find(query, projection)
    
    def close(self):
        self.client.close()
    
class MongoUpdateDataTransformer(DataTransformer):
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
            source = self.get_attr(field, 'source')
            val = row.get(source) or row.get(source.lower())
            if val:
                if isinstance(val, list):
                    val = ' '.join(val)
                    
                isdigit = self.get_attr(field, "isdigit")
                if isdigit:
                    val = self._clean_digit(val)
                    
                indexed = self.get_attr(field, "indexed")
                name = self.get_attr(field, 'name')
                if indexed:
                    query = { name : val }
                op = self.get_attr(field, 'op')
                ops = update.get(op)
                
                if ops:
                    ops.update({ name : val })
                else:
                    update[op] = { name : val }
                    
                clean = self.get_attr(field, "clean")
                if clean:
                    update.get(op).update(\
                        { '__' + name : self._clean_str(val) })
                           
        if query:
            return query,update
    
    def transform(self, row):
        mapping = self.config.settings.transformer.mapping
        if isinstance(mapping, list):
            data = {}
            for conf in mapping:
                fields = self.get_attr(conf, 'fields')
                result = self._transform(fields, row)
                if result:
                    tag = self.get_attr(conf, 'tag')
                    data[tag] = result
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
        self.collection = self._get_collection(mongodb)

    def _get_collection(self, conf):
        db = self.get_attr(conf, 'db')
        collection = self.get_attr(conf, 'collection')
        logging.debug('db:%s' % db)
        logging.debug('collection:%s' % collection)
        db = self.client[db]
        username = self.get_attr(conf, 'username')
        if username:
            password = self.get_attr(conf, 'password')
            authsource = self.get_attr(conf, 'authenticationDatabase')
            db.authenticate(username, password, source=authsource)
        collection = db[collection]
        index_key = self.get_attr(conf, 'indexKey')
        if index_key:
            collection.ensure_index(index_key, unique=True)
        return collection

    def load(self, datacol):
        docs = []
        for data in datacol:
            doc = SON([(name,val) for name,val in data
                       if val is not None])
            docs.append(doc)
        self.collection.insert_many(docs)

    def close(self):
        self.client.close()

class MongoUpdateDataLoader(MongoDataLoader):
    def __init__(self, config):
        mongodb = config.settings.loader.mongodb
        logging.debug('*' * 30)
        logging.debug('MongoUpdateDataLoader init:')
        logging.debug('host:%s, port:%s' % (mongodb.host, mongodb.port))

        self.client = MongoClient(mongodb.host, mongodb.port)
        if hasattr(mongodb, 'dbs'):
            self.collections = {}
            for conf in mongodb.dbs:
                tag = self.get_attr(conf, 'tag')
                logging.debug('-' * 30)
                logging.debug('tag:%s' % tag)
                collection = self._get_collection(conf)
                self.collections[tag] = collection
        else:
            self.collection = self._get_collection(mongodb)
                
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
