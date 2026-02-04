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

public class QueryIcebergTable {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints4");
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.noRestart());

        // Add Iceberg REST catalog matching Fluss configuration
        String icebergCatalog = 
                "CREATE CATALOG iceberg_catalog WITH (\n" +
                "  'type' = 'iceberg',\n" +
                "  'catalog-type'='rest',\n" +
                // "  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',\n" +
                "  'uri' = 'http://localhost:8181',\n" +
                "  'warehouse' = 's3://warehouse/',\n" +
                "  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',\n" +
                "  's3.endpoint' = 'http://localhost:9000',\n" +
                "  's3.path-style-access' = 'true',\n" +
                "  'client.region' = 'us-east-1',\n" +
                "  's3.access-key-id' = 'admin',\n" +
                "  's3.secret-access-key' = 'password'\n" +
                ")";
        tEnv.executeSql(icebergCatalog).print();


        // Print Flink table from Iceberg: (does not include data from Fluss memory store, only data that has been flushed to Iceberg)
        String selectFluss =
                "SELECT * \n" +
                "FROM iceberg_catalog.fluss.fluss_user8\n" +
                ";"
        ;
        tEnv.executeSql(selectFluss).print();
    }
}
