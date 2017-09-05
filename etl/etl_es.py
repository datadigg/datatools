# -*- coding: utf-8 -*-
import os, logging, datetime, json
from elasticsearch import Elasticsearch, helpers
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
    
    def getval(self, row, field):
        val = super(ElasticsearchDataTransformer, self).getval(row, field)
        field_type = getattr(field, 'type', False)
        if field_type == 'str':
            val = val and str(val) or val
        
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
        self.client = Elasticsearch(es.hosts)
        self._initIndexTemplate()


    def _initIndexTemplate(self):
        if hasattr(self.config.args, 'template_name'):
            template_name = self.config.args.template_name
            if template_name:
                fn = template_name + '-' + self.config.args.profile + '.json'
                with open(os.path.join(self.config.args.conf, fn)) as json_data:
                    template_body = json.load(json_data)
                    self.client.indices.put_template(name = template_name,
                                                     body = template_body)
                

    def load(self, datacol):
        actions = []
        for data in datacol:
            action = {
                '_index' : self.es.index.format(**dict(data)),
                '_type' : self.es.doc_type
            }
            action.update(data)
            actions.append(action)
        
        helpers.bulk(self.client, actions)

    def close(self):
        pass
        
