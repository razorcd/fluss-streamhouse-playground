package com.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkKafkaSQL {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

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
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json',\n" + // Assuming the Kafka messages are JSON
                        "  'json.ignore-parse-errors' = 'true'\n" +
                        ")";
        tEnv.executeSql(createTableSql).print();

    
        String fluxxCatalog = 
                " CREATE CATALOG fluss_catalog \n" +
                " WITH (\n" +
                "  'type' = 'fluss',\n" +
                // "  'default-database' = 'default',\n" +
                "  'bootstrap.servers' = 'localhost:9123'\n" +
                ")";


        tEnv.executeSql(fluxxCatalog).print();
        // tEnv.executeSql("USE CATALOG fluss_catalog").print();

        String dropTableIfExistis = "DROP TABLE IF EXISTS fluss_catalog.fluss.fluss_user;";
        tEnv.executeSql(dropTableIfExistis).print();

        String createFlussTable = "CREATE TABLE fluss_catalog.fluss.fluss_user (\n" + 
                        "  event_id STRING,\n" +
                        "  user_id STRING,\n" +
                        "  event_time TIMESTAMP_LTZ(3)\n" +
                        ");";
        tEnv.executeSql(createFlussTable).print(); 

        String insertIntoFluss = 
                "INSERT INTO fluss_catalog.fluss.fluss_user \n" +
                "SELECT event_id, user_id, event_time \n" +
                "FROM default_catalog.default_database.rawdatastreamTable;";
        tEnv.executeSql(insertIntoFluss);


//      {"event_id": "event2", "user_id":"1002"}
//        String selectSql =
//                "SELECT\n" +
//                "  event_id,\n" +
//                "  user_id,\n" +
//                "  event_time\n" +
//                "FROM rawdatastreamTable\n"
////                "WHERE user_id = '1001'"
//                ;

        // Print Flink table from Fluss:
       String selectFluss =
                "SELECT * \n" +
                "FROM fluss_catalog.fluss.fluss_user\n" +
                ";"
        ;

        Table resultFlussTable = tEnv.sqlQuery(selectFluss);
        DataStream<Row> resultFlussStream = tEnv.toChangelogStream(resultFlussTable);
        resultFlussStream.print();


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
        env.execute("Flink Kafka SQL Job");
    }
}
