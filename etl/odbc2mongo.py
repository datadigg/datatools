# -*- coding: utf-8 -*-
import sys, argparse
import etl, etl_mongo, etl_odbc
from config import Configuration

def main():
        parser = argparse.ArgumentParser(description='ETL(odbc to mongo)')
        parser.add_argument('--conf', required=True)
        parser.add_argument('--profile', required=False)
        parser.add_argument('--dbfile', required=False)
        parser.add_argument('--update', action='store_true', default=False)

        args = parser.parse_args()
        config = Configuration(parser.parse_args())
        extractor = etl_odbc.ODBCDataExtractor(config)
        if args.update:
                transformer = etl_mongo.MongoUpdateDataTransformer(config)
                loader = etl_mongo.MongoUpdateDataLoader(config)
        else:
                transformer = etl.DataTransformer(config)
                loader = etl_mongo.MongoDataLoader(config)
                
        etl.etl(config, extractor, transformer, loader)
        
if __name__ == '__main__':
        main()
