# -*- coding: utf-8 -*-
import os, logging, datetime, json
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionTimeout

from .etl import DataExtractor, DataLoader

logger = logging.getLogger(__name__)


class ElasticsearchDataExtractor(DataExtractor):
    def __init__(self, config):
        es = config.settings.extractor.elasticsearch
        logger.debug('*' * 30)
        logger.debug('ElasticsearchDataExtractor init:')
        logger.debug('hosts:%s' % es.hosts)
        logger.debug('index:%s' % es.index)
        logger.debug('doc_type:%s' % es.doc_type)

        self.config = config
        self.es = es
        self.client = Elasticsearch(es.hosts)

    def getrows(self, top=0):
        body = eval(self.config.args.body)
        logger.debug('execute search body:%s' % body)
        response = self.client.search(index=self.es.index,
                                      doc_type=self.es.doc_type,
                                      body=body)
        return response['hits']['hits']

    def close(self):
        pass
            

class ElasticsearchDataLoader(DataLoader):
    def __init__(self, config):
        es = config.settings.loader.elasticsearch
        logger.debug('*' * 30)
        logger.debug('ElasticsearchDataLoader init:')
        logger.debug('hosts:%s' % es.hosts)
        logger.debug('index:%s' % es.index)
        logger.debug('doc_type:%s' % es.doc_type)

        self.config = config
        self.es = es
        self._init_client(es)
        self._init_index_template(config)
        
        self.current_indices = []

    def _client_args(self, name, kwargs):
        if 'client' in self.es and name in self.es.client:
            kwargs.update(self.es.client[name])
        return kwargs
    
    def _init_client(self, es):
        conn_args = self._client_args('connect', {
            'timeout': 100,
            'max_retries': 3,
            'retry_on_timeout': True
            })
        logger.debug('client connect args: %s' % conn_args)
        self.client = Elasticsearch(es.hosts, **conn_args)

    def _init_index_template(self, config):
        logger.debug('init index template:')
        if hasattr(config.args, 'index_template_name'):
            template_name = config.args.index_template_name
            if template_name:
                fn = template_name + '-' + config.args.profile + '.json'
                logger.debug('template file: %s' % fn)
                with open(os.path.join(config.args.conf, fn)) as json_data:
                    template_body = json.load(json_data)
                    self.client.indices.put_template(name=template_name,
                                                     body=template_body)

    def _prepare_settings(self, index):
        if index not in self.current_indices:
            self.current_indices.append(index)
            
            if not self.client.indices.exists(index):
                    self.client.indices.create(index)
                    
            if hasattr(self.config.args, 'index_settings'):
                index_settings = self.config.args.index_settings
                if index_settings:
                    logger.debug('index settings: %s' % index_settings)
                    self.client.indices.put_settings(
                            index=index, body=index_settings)
        
    def _generate_actions(self, datacol):
        for data in datacol:
            index = self.es.index.format(**dict(data))
            self._prepare_settings(index)
            action = {
                '_index': index,
                '_type': self.es.doc_type
            }
            action.update({name: val for name, val in data
                           if val is not None})
            yield action
    
    def load(self, datacol, callback=None, **kwargs):
        bulk_args = self._client_args('bulk', {})
        logger.debug('client bulk args: %s' % bulk_args)
            
        total = 0
        for success, info in helpers.parallel_bulk(
                self.client, self._generate_actions(datacol), **bulk_args):
            if success:
                total += 1
                if callback:
                    callback({'start_time': kwargs['start_time'],
                              'current': total})
            else:
                raise Exception('doc failed: %s' % info)
        return total

    def optimize(self):
        if self.current_indices:
            index = ','.join(self.current_indices)
            logger.debug('optimize index: %s' % index)
            merge_args = self._client_args('merge', {
                'max_num_segments': 1,
                'ignore_unavailable': True
                })
            logger.debug('client merge args: %s' % merge_args)
            try:   
                self.client.indices.forcemerge(index=index, **merge_args)
            except ConnectionTimeout:
                pass
