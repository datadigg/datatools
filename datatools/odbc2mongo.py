# -*- coding: utf-8 -*-
import sys, argparse

from etl import etl, etl_mongo, etl_odbc
from etl.config import Configuration


def main():
    parser = argparse.ArgumentParser(description='ETL(odbc to mongo)')
    parser.add_argument('--conf-dir', required=True)
    parser.add_argument('--profile', required=False)
    parser.add_argument('--dbfile', required=False)
    parser.add_argument('--optimize', action='store_true', default=False)
    parser.add_argument('--update', action='store_true', default=False)
    parser.add_argument('--settings', required=False, default='{}')

    from settings import configure_logging
    configure_logging()

    args = parser.parse_args()
    config = Configuration(vars(args))
    extractor = etl_odbc.ODBCDataExtractor(config)

    if args.update:
        transformer = etl_mongo.MongoUpdateDataTransformer(config)
        loader = etl_mongo.MongoUpdateDataLoader(config)
    else:
        transformer = etl.SimpleDataTransformer(config)
        loader = etl_mongo.MongoDataLoader(config)
                
    etl.etl(config, extractor, transformer, loader)


if __name__ == '__main__':
    main()
