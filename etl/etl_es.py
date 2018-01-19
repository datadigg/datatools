# -*- coding: utf-8 -*-
import os, logging, datetime, json
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionTimeout
from etl import DataExtractor,DataLoader

class ElasticsearchDataExtractor(DataExtractor):
    def __init__(self, config):
        es = config.settings.extractor.elasticsearch
        logging.debug('*' * 30)
        logging.debug('ElasticsearchDataExtractor init:')
        logging.debug('hosts:%s' % es.hosts)
        logging.debug('index:%s' % es.index)
        logging.debug('doc_type:%s' % es.doc_type)

        self.config = config
        self.es = es
        self.client = Elasticsearch(es.hosts)

    def getrows(self, top=0):
        body = eval(self.config.args.body)
        logging.debug('execute search body:%s' % body)
        response = self.client.search(index=self.es.index,
                                      doc_type=self.es.doc_type,
                                      body=body)
        return response['hits']['hits']

    def close(self):
        pass
            

class ElasticsearchDataLoader(DataLoader):
    def __init__(self, config):
        es = config.settings.loader.elasticsearch
        logging.debug('*' * 30)
        logging.debug('ElasticsearchDataLoader init:')
        logging.debug('hosts:%s' % es.hosts)
        logging.debug('index:%s' % es.index)
        logging.debug('doc_type:%s' % es.doc_type)

        self.es = es
        self._init_client(es)
        self._init_index_template(config)
        
        self.current_indices = []

    def _client_args(self, name, kwargs):
        if 'client' in self.es and name in self.es.client:
            kwargs.update(self.es.client[name]['kwargs'])
        return kwargs
    
    def _init_client(self, es):
        conn_args = self._client_args('connect', {
            'timeout': 100,
            'max_retries': 3,
            'retry_on_timeout': True
            })
        logging.debug('client connect args: %s' % conn_args)
        self.client = Elasticsearch(es.hosts, **conn_args)

    def _init_index_template(self, config):
        logging.debug('init index template:')
        if hasattr(config.args, 'template_name'):
            template_name = config.args.template_name
            if template_name:
                fn = template_name + '-' + config.args.profile + '.json'
                logging.debug('template file: %s' % fn)
                with open(os.path.join(config.args.conf, fn)) as json_data:
                    template_body = json.load(json_data)
                    self.client.indices.put_template(name = template_name,
                                                     body = template_body)

    def _prepare_settings(self, index):
        if not index in self.current_indices:
            self.current_indices.append(index)
            
            if not self.client.indices.exists(index):
                    self.client.indices.create(index)
                    
            if 'settings' in self.es:
                self.client.indices.put_settings(
                        index=index, body=self.es.settings)
        
    def _generate_actions(self, datacol):
        for data in datacol:
            index = self.es.index.format(**dict(data))
            self._prepare_settings(index)
            action = {
                '_index' : index,
                '_type' : self.es.doc_type
            }
            action.update(data)
            yield action
            
    def load(self, datacol):
        bulk_args = self._client_args('bulk', {})
        if bulk_args:
            logging.debug('client bulk args: %s' % bulk_args)
            
        for success, info in helpers.parallel_bulk(self.client,
                              self._generate_actions(datacol), **bulk_args):
            if not success:
                raise Exception('doc failed: %s' % info)

    def optimize(self):
        if self.current_indices:
            index = ','.join(self.current_indices)
            logging.debug('optimize index: %s' % index)
            merge_args = self._client_args('merge', {
                'max_num_segments': 1,
                'ignore_unavailable': True
                })
            logging.debug('client merge args: %s' % merge_args)
            try:   
                self.client.indices.forcemerge(index=index, **merge_args)
            except ConnectionTimeout as e:
                pass
        
