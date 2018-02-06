# -*- coding: utf-8 -*-
import sys, argparse

from etl import etl, etl_mongo, etl_odbc
from etl.config import Configuration

def main():
        parser = argparse.ArgumentParser(description='ETL(mongo to odbc)')
        parser.add_argument('--conf', required=True)
        parser.add_argument('--profile', required=True)
        parser.add_argument('--query', required=True)
        parser.add_argument('--settings', required=False, default='{}')

        from settings import configure_logging
        configure_logging()
        
        config = Configuration(vars(parser.parse_args()))
        extractor = etl_mongo.MongoDataExtractor(config)
        transformer = etl.SimpleDataTransformer(config)
        loader = etl_odbc.ODBCDataLoader(config)
        etl.etl(config, extractor, transformer, loader)
        
if __name__ == '__main__':
        main()
