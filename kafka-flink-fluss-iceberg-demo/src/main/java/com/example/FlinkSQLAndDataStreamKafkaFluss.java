package com.example;

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

import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.sink.serializer.RowDataSerializationSchema;
import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.sink.KafkaSink;

public class FlinkSQLAndDataStreamKafkaFluss {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(15000);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints2");
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
            3,  // number of restart attempts
            Time.seconds(10)  // delay between restarts
        ));
        
        // Kafka source
        String createTableSql =
                "CREATE TABLE rawdatastreamTable (\n" +
                        "  event_id STRING,\n" +
                        "  user_id STRING,\n" +
                        "  event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',\n" +
                        // Define Event Time and Watermark Strategy
                        "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'rawdatastream',\n" +
                        "  'properties.bootstrap.servers' = 'localhost:9092',\n" + // Replace with your Kafka address
                        "  'properties.group.id' = 'flink-consumer-group4',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json',\n" + // Assuming the Kafka messages are JSON
                        "  'json.ignore-parse-errors' = 'true'\n" +
                        ")";
        tEnv.executeSql(createTableSql);

    
        // From Kafka to Fluss
        String flussCatalog = 
                " CREATE CATALOG fluss_catalog \n" +
                " WITH (\n" +
                "  'type' = 'fluss',\n" +
                // "  'default-database' = 'default',\n" +
                "  'bootstrap.servers' = 'localhost:9123'\n" +
                ")";


        tEnv.executeSql(flussCatalog);
        tEnv.executeSql("USE CATALOG fluss_catalog");

        // String dropTableIfExistis = "DROP TABLE IF EXISTS fluss_catalog.fluss.fluss_user2;";
        // tEnv.executeSql(dropTableIfExistis).print();

        String createFlussTable = "CREATE TABLE IF NOT EXISTS fluss_catalog.fluss.fluss_user2 (\n" + 
                        "  event_id STRING,\n" +
                        "  user_id STRING,\n" +
                        "  event_time TIMESTAMP_LTZ(3),\n" +
                        "  PRIMARY KEY (`user_id`) NOT ENFORCED,\n" +   // this add `upsert` capability instead of append-only. (note: Kafka is append-only)
                        "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
                        ")\n" +
                        "WITH (\n" +
                        // "  'scan.startup.mode' = 'latest'\n" +
                        ");";
        tEnv.executeSql(createFlussTable); 

        String insertIntoFluss = 
                "INSERT INTO fluss_catalog.fluss.fluss_user2 \n" +
                "SELECT event_id, user_id, event_time \n" +
                "FROM default_catalog.default_database.rawdatastreamTable;";
        tEnv.executeSql(insertIntoFluss);



               // String selectSqlTestKafka1 =
        //         "SELECT\n" +
        //                 "  event_id,\n" +
        //                 "  user_id,\n" +
        //                 "  event_time\n" +
        //                 "  timestamp1\n" +
        //                 "FROM rawdatastreamSinkTable;";
        // System.out.print("Flink kafka sync table:");
        // tEnv.executeSql(selectSqlTestKafka1);


        // Create a FlussSource using the builder pattern
        FlussSource<Event> flussSource = FlussSource.<Event>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("fluss_user2")
                .setProjectedFields("event_id", "user_id")
                .setStartingOffsets(OffsetsInitializer.latest()) // should continue from checkpoint (must test)
                .setScanPartitionDiscoveryIntervalMs(1000L)
                .setDeserializationSchema(new EventDeserializationSchemaFluss())
                .build();

        DataStreamSource<Event> stream =
                env.fromSource(flussSource, WatermarkStrategy.forMonotonousTimestamps(), "Fluss Orders Source");

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

        // stream.print();
        stream.sinkTo(kafkaSink).name("Kafka Sink");

        env.execute("Flink DataStream Kafka to Fluss Example");











    }
}
