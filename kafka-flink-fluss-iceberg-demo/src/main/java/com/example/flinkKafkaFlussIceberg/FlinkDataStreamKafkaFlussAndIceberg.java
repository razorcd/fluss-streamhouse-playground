package com.example.flinkKafkaFlussIceberg;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import org.apache.fluss.flink.sink.FlussSink;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.source.FlussSource;
import org.apache.fluss.flink.source.enumerator.initializer.OffsetsInitializer;

import com.example.Event;
import com.serdes.EventDeserializationSchemaFluss;
import com.serdes.EventDeserializationSchemaKafka;
import com.serdes.EventSerializationSchemaFluss;
import com.serdes.EventSerializationSchemaKafka;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;

public class FlinkDataStreamKafkaFlussAndIceberg {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints4");
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.noRestart());


        // Kafka to Fluss via DataStream API
        KafkaSource<Event> kafkaSource = KafkaSource.<Event>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("rawdatastream")
                .setGroupId("flink-consumer-group4")
                .setStartingOffsets(org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new EventDeserializationSchemaKafka())
                .build();

        DataStreamSource<Event> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");
        // kafkaStream.print();

        FlussSink<Event> flussSink = FlussSink.<Event>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("fluss_user8")
                .setSerializationSchema(new EventSerializationSchemaFluss())
                .build();

        kafkaStream.sinkTo(flussSink).name("Fluss Sink");


        // Fluss to Kafka via DataStream API
        FlussSource<Event> flussSource = FlussSource.<Event>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("fluss_user8")
                .setProjectedFields("event_id", "user_id")
                .setStartingOffsets(OffsetsInitializer.latest()) // should continue from checkpoint (must test)
                .setScanPartitionDiscoveryIntervalMs(1000L)
                .setDeserializationSchema(new EventDeserializationSchemaFluss())
                .build();

        DataStreamSource<Event> stream = env.fromSource(flussSource, WatermarkStrategy.forMonotonousTimestamps(), "Fluss Orders Source");
        // stream.print();

        KafkaSink<Event> kafkaSink = KafkaSink.<Event>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder()
                                .setTopic("rawdatastream3")
                                .setValueSerializationSchema(new EventSerializationSchemaKafka())
                                .build()
                )
                .build();

        stream.sinkTo(kafkaSink).name("Kafka Sink");
        // stream.print();

        env.execute("Flink DataStream Kafka to Fluss Example");
    }
}
