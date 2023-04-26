dependencies {
    implementation 'org.apache.flink:flink-table-api-java:1.16.1'
    implementation 'org.apache.flink:flink-table-planner-blink_2.12:1.16.1'
    implementation 'org.apache.flink:flink-streaming-java_2.12:1.16.1'
    implementation 'org.apache.flink:flink-connector-filesystem_2.12:1.16.1'
    implementation 'org.apache.flink:flink-connector-s3:1.12.2'
}

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

// set up Flink Table environment
EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

// define schema for the CSV file
String[] fieldNames = {"id", "name", "age"};
TypeInformation<?>[] fieldTypes = {Types.INT(), Types.STRING(), Types.INT()};
Schema schema = new Schema().fields(fieldNames, fieldTypes);

// define format for the CSV file
FormatDescriptor format = new FormatDescriptor().csv().fieldDelimiter(",");

// create a Flink Table source for the CSV file
FileSystem fileSystem = new FileSystem().path("s3a://BUCKET_NAME/PATH_TO_FILE.csv");
tEnv.connect(fileSystem).withFormat(format).withSchema(schema).createTemporaryTable("myTable");

// read the CSV file into a Flink Table
Table table = tEnv.from("myTable");

// print the table contents
table.printSchema();
table.select("*").print();

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.Types;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.CsvTableSource;

public class ReadCsvFromS3Example {

    public static void main(String[] args) throws Exception {

        // Create a Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Define the schema for the CSV file
        String[] fieldNames = {"id", "name", "age"};
        TypeInformation[] fieldTypes = {Types.INT(), Types.STRING(), Types.INT()};
        TableSchema schema = new TableSchema(fieldNames, fieldTypes);

        // Create a CsvTableSource with the schema and S3 file location
        CsvTableSource csvSource = CsvTableSource.builder()
                .path("s3a://my-bucket/my-csv-file.csv")
                .fieldDelimiter(",")
                .ignoreFirstLine()
                .ignoreParseErrors()
                .schema(schema)
                .property("access.key", "your_access_key_here")
                .property("secret.key", "your_secret_key_here")
                .build();

        // Register the CsvTableSource as a table in the table environment
        tableEnv.registerTableSource("my_table", csvSource);

        // Query the table and print the results
        Table result = tableEnv.sqlQuery("SELECT name, age FROM my_table WHERE age >= 18");
        result.printSchema();
        result.print();

        // Execute the Flink job
        env.execute();
    }
}


CREATE TABLE my_table (
    id INT,
    name STRING,
    age INT
) WITH (
    'connector' = 'filesystem',
    'path' = 's3a://my-bucket/my-csv-file.csv',
    'format' = 'csv',
    'csv.field-delimiter' = ',',
    'csv.ignore-first-line' = 'true',
    'csv.allow-comments' = 'true',
    'csv.ignore-parse-errors' = 'true',
    's3.access-key' = 'YOUR_ACCESS_KEY',
    's3.secret-key' = 'YOUR_SECRET_KEY'
);
