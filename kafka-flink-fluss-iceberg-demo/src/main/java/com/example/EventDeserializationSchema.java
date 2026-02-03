package com.example;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.alibaba.fluss.flink.source.deserializer.FlussDeserializationSchema;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.RowType;

public class EventDeserializationSchema implements FlussDeserializationSchema<Event> {
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