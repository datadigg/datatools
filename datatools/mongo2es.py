# -*- coding: utf-8 -*-
import sys, argparse

from etl import etl, etl_mongo, etl_es
from etl.config import Configuration


def main():
    parser = argparse.ArgumentParser(description='ETL(mongo to es)')
    parser.add_argument('--conf', '--conf-dir', required=True)
    parser.add_argument('--query', required=True)
    parser.add_argument('--profile', required=False)
    parser.add_argument('--optimize', action='store_true', default=False)
    parser.add_argument('--index-template-name', required=False)
    parser.add_argument('--index-settings', required=False)
    parser.add_argument('--settings', required=False, default='{}')
        
    from settings import configure_logging
    configure_logging()

    config = Configuration(vars(parser.parse_args()))
    extractor = etl_mongo.MongoDataExtractor(config)
    transformer = etl.SimpleDataTransformer(config)
    loader = etl_es.ElasticsearchDataLoader(config)
    etl.etl(config, extractor, transformer, loader)


if __name__ == '__main__':
    main()
