# -*- coding: utf-8 -*-
from pymongo import MongoClient
from bson.son import SON

from .config import get_config
from .etl import DataExtractor, DataLoader, SimpleDataTransformer

import logging
import datetime
import funcutils

logger = logging.getLogger(__name__)


class MongoDataExtractor(DataExtractor):
    def __init__(self, config):
        mongodb = config.settings.extractor.mongodb
        logger.debug('*' * 30)
        logger.debug('MongoDataExtractor init:')
        logger.debug('host:%s, port:%s' % (mongodb.host, mongodb.port))
        logger.debug('db:%s' % mongodb.db)
        logger.debug('collection:%s' % mongodb.collection)
        
        self.config = config
        self.mongodb = mongodb
        self.client = MongoClient(mongodb.host, mongodb.port)
        self.db = self.client[mongodb.db]
        if mongodb.username:
            self.db.authenticate(mongodb.username, mongodb.password,
                                 source=mongodb.authenticationDatabase)
        self.collection = self.db[mongodb.collection]

    def getrows(self, top=0):
        query = eval(self.config.args.query)
        logger.debug('execute query:%s' % query)
        if hasattr(self.mongodb, 'projection'):
            projection = eval(self.mongodb.projection)
            return self.collection.find(query, projection)
        else:
            return self.collection.find(query)

    def close(self):
        self.client.close()


class MongoUpdateDataTransformer(SimpleDataTransformer):
    def __init__(self, config, handlers=None):
        super(MongoUpdateDataTransformer, self).__init__(config, handlers)

    @staticmethod
    def _clean_digit(val):
        return ''.join(c for c in val if c.isdigit())

    @staticmethod
    def _clean_str(val):
        return ''.join((e.isalnum() and e or '') for e in val).upper()
    
    def _transform(self, fields, row):
        query = {}
        update = {}
        for field in fields:
            val = self.get_val(row, field)
            if val:
                if isinstance(val, list):
                    val = ' '.join(val)
                    
                isdigit = field.get('isdigit')
                if isdigit:
                    val = self._clean_digit(val)
                    
                indexed = field.get('indexed')
                name = field.get('name')
                if indexed:
                    query = {name: val}
                op = field.get('op')
                ops = update.get(op)
                
                if ops:
                    ops.update({name: val})
                else:
                    update[op] = {name: val}
                    
                clean = field.get("clean")
                if clean:
                    update.get(op).update({'__' + name: self._clean_str(val)})
                           
        if query:
            return query, update
    
    def transform(self, row):
        mapping = self.config.settings.transformer.mapping
        if isinstance(mapping, list):
            data = {}
            for conf in mapping:
                fields = get_config(conf, 'fields')
                result = self._transform(fields, row)
                if result:
                    tag = get_config(conf, 'tag')
                    data[tag] = result
        else:
            data = self._transform(mapping.fields, row)
        return data


class MongoDataLoader(DataLoader):
    def __init__(self, config):
        mongodb = config.settings.loader.mongodb
        logger.debug('*' * 30)
        logger.debug('MongoDataLoader init:')
        logger.debug('host:%s, port:%s' % (mongodb.host, mongodb.port))

        self.client = MongoClient(mongodb.host, mongodb.port)
        self.collection = self._get_collection(mongodb)
        self.commit_size = get_config(config.settings.loader, 'autoCommitSize') or 1000

    def _get_collection(self, conf):
        db = get_config(conf, 'db')
        collection = get_config(conf, 'collection')
        logger.debug('db:%s' % db)
        logger.debug('collection:%s' % collection)
        db = self.client[db]
        username = get_config(conf, 'username')
        if username:
            password = get_config(conf, 'password')
            authsource = get_config(conf, 'authenticationDatabase')
            db.authenticate(username, password, source=authsource)
        collection = db[collection]
        index_key = get_config(conf, 'indexKey')
        if index_key:
            collection.ensure_index(index_key, unique=True)
        return collection

    def load(self, datacol, callback=None, **kwargs):
        total = 0
        docs = []
        for data in datacol:
            total += 1
            if callback:
                callback({'start_time': kwargs['start_time'],
                          'current': total})
            doc = SON([(name, val) for name, val in data
                       if val is not None])
            docs.append(doc)
            if len(docs) == self.commit_size:
                self.collection.insert_many(docs)
                docs[:] = []
        if docs:
            self.collection.insert_many(docs)
        return total

    def close(self):
        self.client.close()


class MongoUpdateDataLoader(MongoDataLoader):
    def __init__(self, config):
        mongodb = config.settings.loader.mongodb
        logger.debug('*' * 30)
        logger.debug('MongoUpdateDataLoader init:')
        logger.debug('host:%s, port:%s' % (mongodb.host, mongodb.port))

        self.client = MongoClient(mongodb.host, mongodb.port)
        if hasattr(mongodb, 'dbs'):
            self.collections = {}
            for conf in mongodb.dbs:
                tag = get_config(conf, 'tag')
                logger.debug('-' * 30)
                logger.debug('tag:%s' % tag)
                collection = self._get_collection(conf)
                self.collections[tag] = collection
        else:
            self.collection = self._get_collection(mongodb)
                
    def load(self, datacol, callback=None, **kwargs):
        total = 0
        for data in datacol:
            total += 1
            if callback:
                callback({'start_time': kwargs['start_time'],
                          'current': total})
            if isinstance(data, dict):
                for key, (query, update) in data.iteritems():
                    self.collections[key].update(query, update, upsert=True)
            else:
                query, update = data
                self.collection.update(query, update, upsert=True)
        return total
    
    def close(self):
        self.client.close()
