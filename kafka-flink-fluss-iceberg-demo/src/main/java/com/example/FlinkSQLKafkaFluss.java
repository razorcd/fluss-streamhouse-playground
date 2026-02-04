package com.example;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;

public class FlinkSQLKafkaFluss {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints1");
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
                        "  'properties.group.id' = 'flink-consumer-group3',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json',\n" + // Assuming the Kafka messages are JSON
                        "  'json.ignore-parse-errors' = 'true'\n" +
                        ")";
        tEnv.executeSql(createTableSql);

    
        // From Kafka to Fluss
        String fluxxCatalog = 
                " CREATE CATALOG fluss_catalog \n" +
                " WITH (\n" +
                "  'type' = 'fluss',\n" +
                // "  'default-database' = 'default',\n" +
                "  'bootstrap.servers' = 'localhost:9123'\n" +
                ")";


        tEnv.executeSql(fluxxCatalog);
        tEnv.executeSql("USE CATALOG fluss_catalog");

        String dropTableIfExistis = "DROP TABLE IF EXISTS fluss_catalog.fluss.fluss_user;";
        tEnv.executeSql(dropTableIfExistis).print();

        String createFlussTable = "CREATE TABLE IF NOT EXISTS fluss_catalog.fluss.fluss_user (\n" + 
                        "  event_id STRING,\n" +
                        "  user_id STRING,\n" +
                        "  event_time TIMESTAMP_LTZ(3),\n" +
                        // "  PRIMARY KEY (`user_id`) NOT ENFORCED,\n" +   // this add `upsert` capability instead of append-only. (note: Kafka is append-only)
                        "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
                        ")\n" +
                        "WITH (\n" +
                        "  'scan.startup.mode' = 'latest'\n" + // should continue from checkpoint (must test)
                        ");";
        tEnv.executeSql(createFlussTable); 

        String insertIntoFluss = 
                "INSERT INTO fluss_catalog.fluss.fluss_user \n" +
                "SELECT event_id, user_id, event_time \n" +
                "FROM default_catalog.default_database.rawdatastreamTable;";
        // tEnv.executeSql(insertIntoFluss);

//      {"event_id": "event10", "user_id":"1100"}
//        String selectSql =
//                "SELECT\n" +
//                "  event_id,\n" +
//                "  user_id,\n" +
//                "  event_time\n" +
//                "FROM rawdatastreamTable\n"
////                "WHERE user_id = '1001'"
//                ;

        // Print Flink table from Fluss:
//        String selectFluss =
//                 "SELECT * \n" +
//                 "FROM fluss_catalog.fluss.fluss_user\n" +
//                 ";"
//         ;
//         tEnv.executeSql(selectFluss).print();
//         env.execute("Flink Kafka to Fluss SQL Job");


        // From Fluss to Kafka sink
        tEnv.executeSql("USE CATALOG default_catalog");

        String createKafkaTable2Sql =
                "CREATE TABLE rawdatastream3 (\n" +
                        "  event_id STRING,\n" +
                        "  user_id STRING,\n" +
                        "  event_time TIMESTAMP_LTZ(3),\n" +
                        "  timestamp1 TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',\n" +
                        // Define Event Time and Watermark Strategy
                        "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'rawdatastream3',\n" +
                        "  'properties.bootstrap.servers' = 'localhost:9092',\n" + // Replace with your Kafka address
                        "  'properties.group.id' = 'flink-consumer-group-test2',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json',\n" + // Assuming the Kafka messages are JSON
                        // "  'json.ignore-parse-errors' = 'true',\n" +
                        "  'sink.partitioner' = 'fixed',\n" +
                        "  'key.format' = 'raw',\n" +
                        "  'key.fields' = 'user_id'\n" +
                        ")";
        tEnv.executeSql(createKafkaTable2Sql);


        String insertFromFlussToKafkaSql = 
                "INSERT INTO rawdatastream3\n" +
                        "SELECT event_id, user_id, event_time, CURRENT_TIMESTAMP\n" +
                        "FROM fluss_catalog.fluss.fluss_user";
        
        // Create StatementSet to execute both INSERT statements together
        tEnv.createStatementSet()
            .addInsertSql(insertFromFlussToKafkaSql)
            .addInsertSql(insertIntoFluss)
            .execute();


        // String selectSqlTestKafka1 =
        //         "SELECT\n" +
        //                 "  event_id,\n" +
        //                 "  user_id,\n" +
        //                 "  event_time\n" +
        //                 "  timestamp1\n" +
        //                 "FROM rawdatastreamSinkTable;";
        // System.out.print("Flink kafka sync table:");
        // tEnv.executeSql(selectSqlTestKafka1);



        // // Queries the output Kafka topic for confirmation.
        // String createTableSqlTestKafka =
        //         "CREATE TABLE rawdatastreamSinkTable2 (\n" +
        //                 "  event_id STRING,\n" +
        //                 "  user_id STRING,\n" +
        //                 "  event_time TIMESTAMP_LTZ(3),\n" +
        //                 "  timestamp1 TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',\n" +
        //                 // Define Event Time and Watermark Strategy
        //                 "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
        //                 ") WITH (\n" +
        //                 "  'connector' = 'kafka',\n" +
        //                 "  'topic' = 'rawdatastream3',\n" +
        //                 "  'properties.bootstrap.servers' = 'localhost:9092',\n" + // Replace with your Kafka address
        //                 "  'properties.group.id' = 'flink-consumer-group-test1',\n" +
        //                 "  'scan.startup.mode' = 'earliest-offset',\n" +
        //                 "  'format' = 'json',\n" + // Assuming the Kafka messages are JSON
        //                 "  'json.ignore-parse-errors' = 'true'\n" +
        //                 ")";
        // tEnv.executeSql(createTableSqlTestKafka);

        // String selectSqlTestKafka =
        //         "SELECT\n" +
        //                 "  event_id,\n" +
        //                 "  user_id,\n" +
        //                 "  event_time\n" +
        //                 "  timestamp1\n" +
        //                 "FROM rawdatastreamSinkTable2;";
        // System.out.print("Kafka output topic:");
        // tEnv.executeSql(selectSqlTestKafka).print();



        // Table resultFlussTable = tEnv.sqlQuery(selectFluss);
        // DataStream<Row> resultFlussStream = tEnv.toChangelogStream(resultFlussTable);
        // resultFlussStream.print();


        // Print Flink table from Kafka:
        // tEnv.executeSql("USE CATALOG default_catalog").print();

        // String selectSql2 =
        //         "SELECT\n" +
        //                 "COUNT(*) AS events_count,\n" +
        //                 "user_id\n" +
        //                 "FROM rawdatastreamTable\n" +
        //                 "WHERE user_id <> '1001'\n" +
        //                 "GROUP BY user_id\n" +
        //                 ";"
        //         ;

        // Table resultTable = tEnv.sqlQuery(selectSql2);

        // // Convert the unbounded table with aggregation to a changelog DataStream
        // DataStream<Row> resultStream = tEnv.toChangelogStream(resultTable);

        // // Process the stream
        // resultStream.print();

        // Execute the Flink job
        // env.execute("Flink Kafka SQL Job");
    }
}
