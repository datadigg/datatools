import sys, os, yaml
import logging, logging.config

BASE_DIR = os.path.dirname(__file__)

def configure_logging(yml='logging.yml'):
    loggingfilepath = os.path.join(BASE_DIR, yml)
    logging.config.dictConfig(yaml.load(open(loggingfilepath,'r')))
