
# Flink demo of Streamhouse pipeline

```
Kafka -> FlinkSQL -> Fluss -> FlinkSQL -> Kafka
```

## Run:

- Flink: http://localhost:8083
- `docker exec -ti kafka-broker bash -c "kafka-topics -bootstrap-server localhost:9092 --create --topic rawdatastream"`
- `docker exec -ti kafka-broker bash -c "kafka-console-producer -bootstrap-server localhost:9092 --topic rawdatastream"`
    - `{"event_id": "event2", "user_id":"1002"}`
- `docker exec -ti kafka-broker bash -c "kafka-console-consumer -bootstrap-server localhost:9092 --topic rawdatastream2 --from-beginning"`
- start the java app on classes from `src`. Note you must start Java with `--add-opens=java.base/java.nio=ALL-UNNAMED`. For VSCode this is set in `.vscode/launch.json`.
