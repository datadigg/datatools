# -*- coding: utf-8 -*-
import sys, argparse
import etl, etl_mongo, etl_es
from config import Configuration

def main():
        parser = argparse.ArgumentParser(description='ETL(mongo to es)')
        parser.add_argument('--conf', required=True)
        parser.add_argument('--profile', required=True)
        parser.add_argument('--query', required=True)
        parser.add_argument('--template-name', required=False)
        parser.add_argument('--settings', required=False, default='{}')
        
        config = Configuration(parser.parse_args())
        extractor = etl_mongo.MongoDataExtractor(config)
        transformer = etl_es.ElasticsearchDataTransformer(config)
        loader = etl_es.ElasticsearchDataLoader(config)
        etl.etl(config, extractor, transformer, loader)
        
if __name__ == '__main__':
        main()
