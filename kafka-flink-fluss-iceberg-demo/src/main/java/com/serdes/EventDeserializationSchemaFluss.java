package com.serdes;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.fluss.flink.source.deserializer.FlussDeserializationSchema;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;

import com.example.Event;

public class EventDeserializationSchemaFluss implements FlussDeserializationSchema<Event> {
    @Override
    public void open(InitializationContext context) throws Exception {
        // Initialization code if needed
    }

    @Override
    public Event deserialize(LogRecord record) throws Exception {
        InternalRow row = record.getRow();

        // Extract fields from the row
        String eventId = row.getString(0).toString();
        String userId = row.getString(1).toString();
        // Long eventTime = row.getLong(2);

        // Create and return your custom object
        return new Event(eventId, userId);
    }

    @Override
    public TypeInformation<Event> getProducedType(RowType rowSchema) {
        return TypeInformation.of(Event.class);
    }
}