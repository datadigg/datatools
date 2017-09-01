# -*- coding: utf-8 -*-
import sys, argparse
import etl, etl_mongo
from config import Configuration

def main():
        parser = argparse.ArgumentParser(description='ETL(mongo to mongo)')
        parser.add_argument('--conf', required=True)
        parser.add_argument('--profile', required=True)
        parser.add_argument('--query', required=True)
        parser.add_argument('--update', action='store_true', default=False)

        args = parser.parse_args()
        config = Configuration(args)
        extractor = etl_mongo.MongoDataExtractor(config)
        if args.update:
                transformer = etl_mongo.MongoUpdateDataTransformer(config)
                loader = etl_mongo.MongoUpdateDataLoader(config)
        else:
                transformer = etl.SimpleDataTransformer(config)
                loader = etl_mongo.MongoDataLoader(config)
                
        etl.etl(config, extractor, transformer, loader)
        
if __name__ == '__main__':
        main()
