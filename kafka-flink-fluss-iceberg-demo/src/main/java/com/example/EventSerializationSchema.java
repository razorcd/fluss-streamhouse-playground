package com.example;

import org.apache.flink.api.common.serialization.SerializationSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class EventSerializationSchema implements SerializationSchema<Event> {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(Event event) {
        try {
            ObjectNode json = objectMapper.createObjectNode();
            json.put("event_id", event.event_id);
            json.put("user_id", event.user_id);
            if (event.event_time != null) {
                json.put("event_time", event.event_time);
            }
            return objectMapper.writeValueAsBytes(json);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize Event to JSON", e);
        }
    }
}
