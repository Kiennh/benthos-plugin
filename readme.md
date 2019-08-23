#### [Benthos](https://github.com/Jeffail/benthos) output plugin to clickhouse

 - Using [clickhouse go client](https://github.com/kshvakov/clickhouse)


##### Build

```
 go build -o cmd/benthos .

```

##### Examples 

 - Create clickhouse table

```sql
CREATE TABLE IF NOT EXISTS 
    example (hitmiss String, client_ip String, status Int32, timestamp DateTime) 
    engine=Memory;

```

 - Using plugin 

```
...
output:
  type: clickhouse
  plugin:
    connection_string: "tcp://localhost:9000"
    batch_size: 10
    query: "insert into sample(hitmiss, client_ip, status, timestamp) values(?, ?, ?, ?)"
    columns:
      - hitmiss
      - client_ip
      - status$floatToInt32
      - timestamp$stringToDate$2006-01-02T15:04:05.000Z

```
