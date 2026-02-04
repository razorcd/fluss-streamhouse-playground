package com.serdes;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.Event;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class EventDeserializationSchemaKafka implements DeserializationSchema<Event> {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Event deserialize(byte[] message) throws IOException {
        JsonNode json = objectMapper.readTree(message);
        
        Event event = new Event();
        event.event_id = json.has("event_id") ? json.get("event_id").asText() : null;
        event.user_id = json.has("user_id") ? json.get("user_id").asText() : null;
        if (json.has("event_time") && !json.get("event_time").isNull()) {
            event.event_time = json.get("event_time").asLong();
        }
        
        return event;
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
