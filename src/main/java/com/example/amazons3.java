import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks;
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// Specify the S3 file path
Path s3FilePath = new Path("s3://<bucket>/<path>/<file>.csv");

// Define the CSV table schema
TableSchema schema = TableSchema.builder()
  .field("id", DataTypes.STRING())
  .field("name", DataTypes.STRING())
  .field("age", DataTypes.INT())
  .build();

// Define the watermark strategy (if applicable)
WatermarkStrategy<Row> watermarkStrategy = new BoundedOutOfOrderTimestamps(5000);

// Configure the FileSystem and OldCsv descriptors
tableEnv.connect(new FileSystem().path(s3FilePath.toString()))
  .withFormat(new OldCsv()
    .fieldDelimiter(",")
    .lineDelimiter("\n")
    .deriveSchema())
  .withSchema(new Schema()
    .field("id", DataTypes.STRING())
    .field("name", DataTypes.STRING())
    .field("age", DataTypes.INT()))
  .createTemporaryTable("csv_table");

// Get the DynamicTableSource from the temporary table
Table csvTable = tableEnv.from("csv_table");
DynamicTableSource csvTableSource = TableFactoryService.find(
  DynamicTableSourceFactory.class,
  "csv"
).createDynamicTableSource(csvTable.getSchema());

// Register the DynamicTableSource as a table
tableEnv.registerTableSource("csv_table", csvTableSource);

// Set the watermark strategy (if applicable)
if (watermarkStrategy != null) {
  tableEnv.registerWatermarkStrategy("csv_table", watermarkStrategy);
}

// Query the CSV table and print the results
Table resultTable = tableEnv.sqlQuery("SELECT * FROM csv_table");
tableEnv.toRetractStream(resultTable, Row.class).print();


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
