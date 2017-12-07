# -*- coding: utf-8 -*-
import os, logging, datetime, json
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionTimeout
from etl import DataExtractor,DataLoader,SimpleDataTransformer

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

class ElasticsearchDataTransformer(SimpleDataTransformer):
    def __init__(self, config):
        super(ElasticsearchDataTransformer, self).__init__(config)
    
    def get_val(self, row, field):
        val = super(ElasticsearchDataTransformer, self).get_val(row, field)
        field_type = self.get_attr(field, 'type')
        if field_type == 'str':
            val = val and str(val) or val
        elif field_type == 'date_str':
            fmt = self.get_attr(field, 'format')
            val = val and val.strftime(fmt) or val
        
        return val
            

class ElasticsearchDataLoader(DataLoader):
    def __init__(self, config):
        es = config.settings.loader.elasticsearch
        logging.debug('*' * 30)
        logging.debug('ElasticsearchDataLoader init:')
        logging.debug('hosts:%s' % es.hosts)
        logging.debug('index:%s' % es.index)
        logging.debug('doc_type:%s' % es.doc_type)

        self.config = config
        self.es = es
        self.client = Elasticsearch(
            es.hosts, timeout=100, max_retries=3,
            retry_on_timeout=True)
        
        self._initIndexTemplate()
        
        self.current_indices = []

    def _initIndexTemplate(self):
        logging.debug('init index template:')
        if hasattr(self.config.args, 'template_name'):
            template_name = self.config.args.template_name
            if template_name:
                fn = template_name + '-' + self.config.args.profile + '.json'
                logging.debug('template file: %s' % fn)
                with open(os.path.join(self.config.args.conf, fn)) as json_data:
                    template_body = json.load(json_data)
                    self.client.indices.put_template(name = template_name,
                                                     body = template_body)

    def _prepare_settings(self, index):
        if not index in self.current_indices:
            self.current_indices.append(index)
            if self.es.settings:
                if not self.client.indices.exists(index):
                    self.client.indices.create(index)
                
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
        for success, info in helpers.parallel_bulk(self.client,
                              self._generate_actions(datacol)):
            if not success:
                raise Exception('doc failed: %s' % info)

    def optimize(self):
        if self.current_indices:
            index = ','.join(self.current_indices)
            logging.debug('optimize index: %s' % index)
            try:
                self.client.indices.forcemerge(index=index,
                    max_num_segments=1, ignore_unavailable=True)
            except ConnectionTimeout as e:
                pass
        
