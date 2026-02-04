package com.example;

import com.alibaba.fluss.flink.row.OperationType;
import com.alibaba.fluss.flink.row.RowWithOp;
import com.alibaba.fluss.flink.sink.serializer.FlussSerializationSchema;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.TimestampLtz;

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
