### Benthos plugins

1. Ouput to clickhouse

2. Processor to process geo (Using maxmind database)

3. Processor to parse [user agent](https://github.com/ua-parser/uap-go)


##### Build

```
 go build -o cmd/benthos .

```


#### 1. [Benthos](https://github.com/Jeffail/benthos) output plugin to clickhouse

 - Using [clickhouse go client](https://github.com/kshvakov/clickhouse)


##### Example

 - Create clickhouse table

```sql
CREATE TABLE IF NOT EXISTS
    sample(hitmiss String, client_ip String, status Int32, timestamp DateTime)
    engine=Memory;

```

 - Using plugin

```
...
output:
  type: clickhouse
  plugin:
    connection_string: "tcp://localhost:9000"
    query: "insert into sample(hitmiss, client_ip, status, timestamp) values(?, ?, ?, ?)"
    columns:
      - hitmiss
      - client_ip
      - status$floatToInt32
      - timestamp$stringToDateOrNow$2006-01-02T15:04:05.000Z

```

 - Conncetion string : The connection string

 - Query : insert query

 - Columns: Columns in jsonfields tobe inserted

    - Supported converter: stringToInt32, floatToInt32, floatToUInt32, floatToUInt8, stringToDateOrNow, unixToDateOrNow

#### 2. Process geo location

```
...
    - type: geo
      plugin:
        file: GeoLite2-City.mmdb
        field: client_ip
...
```

 - file: maxmind mmdb file
 - field: field to process


#### 3. Parse user agent

```
...
    - type: useragent
      plugin:
        file: regexes.yaml
        field: user_agent
...
```

 - file: [regex of useragent](https://github.com/ua-parser/uap-core/blob/286809e09706ea891b9434ed875574d65e0ff6b7/regexes.yaml)
 - field: field to process




