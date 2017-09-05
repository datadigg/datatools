# -*- coding: utf-8 -*-
import sys, argparse
import etl, etl_es, etl_odbc
from config import Configuration

def main():
        parser = argparse.ArgumentParser(description='ETL(es to odbc)')
        parser.add_argument('--conf', required=True)
        parser.add_argument('--profile', required=True)
        parser.add_argument('--body', required=True)
        
        config = Configuration(parser.parse_args())
        extractor = etl_es.ElasticsearchDataExtractor(config)
        transformer = etl.SimpleDataTransformer(config)
        loader = etl_odbc.ODBCDataLoader(config)
        etl.etl(config, extractor, transformer, loader)
        
if __name__ == '__main__':
        main()
