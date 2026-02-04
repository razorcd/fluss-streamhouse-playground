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

public class CreateFlussIcebergTable {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints4");
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.noRestart());

        // Fluss table and catalog
        String flussCatalog = 
                " CREATE CATALOG fluss_catalog \n" +
                " WITH (\n" +
                "  'type' = 'fluss',\n" +
                // "  'default-database' = 'default',\n" +
                "  'bootstrap.servers' = 'localhost:9123'\n" +
                ")";

        tEnv.executeSql(flussCatalog).print();
        // tEnv.executeSql("USE CATALOG fluss_catalog").print();


        // // Delete table from Flink:
        // // String dropTableIfExistis = "DROP TABLE IF EXISTS fluss_catalog.fluss.fluss_user4;";
        // // tEnv.executeSql(dropTableIfExistis).print();

        String createFlussTable = "CREATE TABLE IF NOT EXISTS fluss_catalog.fluss.fluss_user8 (\n" + 
                        "  event_id STRING,\n" +
                        "  user_id STRING,\n" +
                        "  event_time TIMESTAMP_LTZ(3),\n" +
                        "  PRIMARY KEY (`user_id`) NOT ENFORCED,\n" +
                        "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
                        ")\n" +
                        "WITH (\n" +
                                // "  'scan.startup.mode' = 'latest'\n" +
                                "  'table.datalake.enabled' = 'true',\n" +
                                "  'table.datalake.freshness' = '5s',\n" +    
                                "  'iceberg.catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',\n" +
                                "  'iceberg.uri' = 'http://rest:8181',\n" +
                                "  'iceberg.warehouse' = 's3://warehouse/',\n" +
                                "  'iceberg.io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',\n" +
                                "  'iceberg.s3.endpoint' = 'http://minio:9000',\n" +
                                "  'iceberg.s3.path-style-access' = 'true',\n" +
                                "  'iceberg.client.region' = 'us-east-1',\n" +
                                "  'iceberg.s3.access-key-id' = 'admin',\n" +
                                "  'iceberg.s3.secret-access-key' = 'password'\n" +
                        ");";
        tEnv.executeSql(createFlussTable).print(); 

        //confirm existance of catalog: http://localhost:8181/v1/namespaces/fluss/tables
        //confirm existance of Iceberg table folder on disk: http://localhost:9001/browser/warehouse/fluss%2F

        
        tEnv.executeSql("SHOW CREATE TABLE fluss_catalog.fluss.fluss_user8").print();
    }
}
