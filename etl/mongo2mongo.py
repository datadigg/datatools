# -*- coding: utf-8 -*-
import sys, argparse
import etl, etl_mongo
from config import Configuration

def main():
        parser = argparse.ArgumentParser(description='ETL(mongo to mongo)')
        parser.add_argument('--conf', required=True)
        parser.add_argument('--profile', required=True)
        parser.add_argument('--query', required=True)
        
        config = Configuration(parser.parse_args())
        extractor = etl_mongo.MongoDataExtractor(config)
        transformer = etl_mongo.MongoDataTransformer(config)
        loader = etl_mongo.MongoDataLoader(config)
        etl.etl(config, extractor, transformer, loader)
        
if __name__ == '__main__':
        main()
