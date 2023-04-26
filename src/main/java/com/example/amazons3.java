plugins {
    id("org.apache.flink") version "1.16.1"
    kotlin("jvm") version "1.5.10"
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.flink:flink-streaming-java_2.12:1.16.1")
    implementation("org.apache.flink:flink-table-api-java-bridge_2.12:1.16.1")
    implementation("org.apache.flink:flink-table-planner_2.12:1.16.1")
    implementation("org.apache.flink:flink-connector-filesystem_2.12:1.16.1")
    implementation("org.apache.flink:flink-s3-fs-hadoop:1.16.1")
    implementation("org.apache.hadoop:hadoop-aws:3.2.2")
}

application {
    mainClass.set("com.example.MyApplication")
}




import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class ReadFromS3Example {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment and table environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // Set the S3 access key and secret
        String accessKey = "<your access key>";
        String secretKey = "<your secret key>";
        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("fs.s3a.access.key", accessKey);
        hadoopConfig.set("fs.s3a.secret.key", secretKey);
        org.apache.hadoop.fs.FileSystem.initialize(hadoopConfig);

        // Define the schema for the CSV file
        String[] fieldNames = {"id", "name", "age"};
        TypeInformation[] fieldTypes = {Types.INT(), Types.STRING(), Types.INT()};
        TableSchema schema = new TableSchema(fieldNames, fieldTypes);
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);

        // Create a CsvTableSource with the schema and S3 file location
        CsvTableSource csvSource = CsvTableSource.builder()
                .path("s3a://my-bucket/my-csv-file.csv")
                .fieldDelimiter(",")
                .ignoreFirstLine()
                .ignoreParseErrors()
                .schema(schema)
                .build();

        // Register the CsvTableSource as a table in the table environment
        tEnv.registerTableSource("my_table", csvSource);

        // Query the table using SQL
        Table result = tEnv.sqlQuery("SELECT name, age FROM my_table WHERE age > 18");

        // Convert the table to a data stream and print the results
        DataStream<Row> outputStream = tEnv.toDataStream(result);
        outputStream.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row row) throws Exception {
                return row.getField(0) + ": " + row.getField(1);
            }
        }).print();

        env.execute("Read from S3 Example");
    }
}

