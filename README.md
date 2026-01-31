- `docker exec -ti kafka-broker bash -c "kafka-topics -bootstrap-server localhost:9092 --create --topic rawdatastream"`
- `docker exec -ti kafka-broker bash -c "kafka-console-producer -bootstrap-server localhost:9092 --topic rawdatastream"`
    - `{"event_id": "event2", "user_id":"1002"}`
    