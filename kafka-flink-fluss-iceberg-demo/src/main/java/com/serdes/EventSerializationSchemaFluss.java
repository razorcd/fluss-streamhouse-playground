package com.serdes;

import org.apache.fluss.flink.row.OperationType;
import org.apache.fluss.flink.row.RowWithOp;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;

import com.example.Event;

public class EventSerializationSchemaFluss implements FlussSerializationSchema<Event> {
    
        private static final long serialVersionUID = 1L;

    @Override
    public void open(InitializationContext context) throws Exception {}

    @Override
        public RowWithOp serialize(Event value) throws Exception {
            GenericRow row = new GenericRow(3);
            row.setField(0, BinaryString.fromString(value.event_id));
            row.setField(1, BinaryString.fromString(value.user_id));
            row.setField(2, TimestampLtz.fromEpochMillis(value.event_time!=null ? value.event_time : 0L));

            return new RowWithOp(row, OperationType.UPSERT);
        }

}
