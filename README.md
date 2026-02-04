
# Streamhouse playground (Fluss + Iceberg)

```
Kafka -> Flink -> Fluss -> Flink -> Kafka
                    v
                 Iceberg
```

## For Fluss without Iceberg:
- run `docker compose up`
- create topics: `rawdatastream` and `rawdatastream3`
- run any of:
    - `com.example.FlinkDataStreamKafkaFluss.java`
    - `com.example.FlinkSQLAndDataStreamKafkaFluss.java`
    - `com.example.FlinkSQLKafkaFluss.java`
- publish some events to `rawdatastream` topic. (`{"event_id": "event10", "user_id":"1160"}`)
- see realtime output events on `rawdatastream3` topic. 
- you can always run a `SELECT * FROM fluss_catalog.fluss.{table_name}` to query the Fluss table.


## For Fluss with Iceberg:
- run `docker compose up`
- run `com.example.flinkKafkaFlussIceberg.CreateFlussIcebergTable.java`
- run `com.example.flinkKafkaFlussIceberg.FlinkDataStreamKafkaFlussAndIceberg.java` 
- publish some events to `rawdatastream` topic. (`{"event_id": "event10", "user_id":"1160"}`)
- see realtime output events on `rawdatastream3` topic. 
- query the Fluss+Iceberg dual table:
    - `docker exec -ti flink-jobmanager ./sql-client`
    - `CREATE CATALOG fluss_catalog8 WITH ('type' = 'fluss', 'bootstrap.servers' = 'fluss-coordinator-server:9123');`
    - `SELECT * FROM fluss_catalog8.fluss.fluss_user8;`
- query only Iceberg table: 
    - run `com.example.flinkKafkaFlussIceberg.QueryIcebergTable.java`

## Tools:
- create Kafka topic: `docker exec -ti kafka-broker bash -c "kafka-topics -bootstrap-server localhost:9092 --create --topic rawdatastream"`
- produce Kafka envet: `docker exec -ti kafka-broker bash -c "kafka-console-producer -bootstrap-server localhost:9092 --topic rawdatastream"`
    - `{"event_id": "event2", "user_id":"1002"}`
- `docker exec -ti kafka-broker bash -c "kafka-console-consumer -bootstrap-server localhost:9092 --topic rawdatastream2 --from-beginning"`
- for starting any of the java apps on classes from `src`, note you must start Java with `--add-opens=java.base/java.nio=ALL-UNNAMED`. For VSCode this is set in `.vscode/launch.json`.
- AKHQ: http://localhost:8082/ui/docker-kafka-server/topic
- Iceberg store - Minio: http://localhost:9001  (user: admin, pass: password)
- Iceberg catalog - Rest: 
    - `curl -X GET http://localhost:8181/v1/namespaces`
    - `curl -X GET http://localhost:8181/v1/namespaces/fluss/tables`
    - `curl -X DELETE http://localhost:8181/v1/namespaces/fluss/tables/fluss_user8` (deletes the table so it can be recreated)
- Flink tiering job: http://localhost:8083/#/job/running


