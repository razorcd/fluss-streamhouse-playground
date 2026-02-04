
# Flink demo of Streamhouse pipeline

```
Kafka -> Flink -> Fluss -> Flink -> Kafka
                    v
                 Iceberg
```

## Run:

- Flink: http://localhost:8083
- `docker exec -ti kafka-broker bash -c "kafka-topics -bootstrap-server localhost:9092 --create --topic rawdatastream"`
- `docker exec -ti kafka-broker bash -c "kafka-console-producer -bootstrap-server localhost:9092 --topic rawdatastream"`
    - `{"event_id": "event2", "user_id":"1002"}`
- `docker exec -ti kafka-broker bash -c "kafka-console-consumer -bootstrap-server localhost:9092 --topic rawdatastream2 --from-beginning"`
- start the java app on classes from `src`. Note you must start Java with `--add-opens=java.base/java.nio=ALL-UNNAMED`. For VSCode this is set in `.vscode/launch.json`.


## To query Fluss and Iceberg duality from Flink:
- run `com.example.flinkKafkaFlussIceberg.CreateFlussIcebergTable.java`
- run `com.example.flinkKafkaFlussIceberg.FlinkDataStreamKafkaFlussAndIceberg.java` and send some events to `rawdatastream` topic. (`{"event_id": "event10", "user_id":"1160"}`)
- `docker exec -ti flink-jobmanager ./sql-client`
- `CREATE CATALOG fluss_catalog8 WITH ('type' = 'fluss', 'bootstrap.servers' = 'fluss-coordinator-server:9123');`
- `SELECT * FROM fluss_catalog8.fluss.fluss_user8;`



## Inspect tools:

- AKHQ: http://localhost:8082/ui/docker-kafka-server/topic
- Iceberg store - Minio: http://localhost:9001  (user: admin, pass: password)
- Iceberg catalog - Rest: 
    - `curl -X GET http://localhost:8181/v1/namespaces`
    - `curl -X GET http://localhost:8181/v1/namespaces/fluss/tables`
    - `curl -X DELETE http://localhost:8181/v1/namespaces/fluss/tables/fluss_user4`
- Flink tiering job: http://localhost:8083/#/job/running


