# Simple ETL Data Tools

Easy-to-use ETL tools based on YAML configuration, compatible with Python 2.x and Python 3.x

## Code Example

```
args = {
    'conf_dir': '/path/confdir'
}

config = Configuration(args)
extractor = ODBCDataExtractor(config)
transformer = SimpleDataTransformer(config)
loader = MongoDataLoader(config)

etl(config, extractor, transformer, loader)
```

## Configuration Examples

settings.yml

### ODBC to Mongodb

```yaml
extractor:
  odbc:
    connstr: connection_string
    table: test_table
transformer:
  mapping:
    fields:
      - { name: field_name1, source: source_name1 }
      - { name: field_name2, source: source_name2 }
loader:
  autoCommitSize: 1000
  mongodb:
    host: localhost
    port : 27017
    authenticationDatabase: 
    username: 
    password: 
    db: test_db
    collection: test
    indexKey: key1
```

### Mongodb to Elasticsearch

```yaml
extractor:
  mongodb:
    host: localhost
    port: 27017
    authenticationDatabase:
    username:
    password:
    db: test_db
    collection: test
    projection: '{ "field1":1, "field2": 1 }'
transformer:
  mapping:
    fields:
      - { name: field_name1, source: source_name1 }
      - { name: field_name2, source: source_name2 }
loader:
  elasticsearch:
    hosts: [ 'localhost:9200' ]
    index: test_idx
    doc_type: test_type
```