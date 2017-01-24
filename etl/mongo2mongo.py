# -*- coding: utf-8 -*-
import sys
import etl, etl_mongo
from config import Configuration

def main(argv):
        config = Configuration(argv)
        extractor = etl_mongo.MongoDataExtractor(config)
        transformer = etl_mongo.MongoDataTransformer(config)
        loader = etl_mongo.MongoDataLoader(config)
        etl.etl(config, extractor, transformer, loader)
        
if __name__ == '__main__':
        main(sys.argv[1:])
