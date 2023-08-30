
$hostsAndPorts = Get-Content -Path ".\hosts.txt" | ConvertFrom-Csv -Header ComputerName, Port

foreach ($entry in $hostsAndPorts) {
    $host = $entry.ComputerName
    $port = $entry.Port
    
    $result = Test-NetConnection -ComputerName $host -Port $port -ErrorAction SilentlyContinue
    
    if ($result.TcpTestSucceeded) {
        Write-Host "Connection to $host on port $port was successful."
        Write-Host "RemoteAddress: $($result.RemoteAddress)"
        Write-Host "RemotePort: $($result.RemotePort)"
        Write-Host "TcpTestSucceeded: $($result.TcpTestSucceeded)"
    } else {
        Write-Host "Connection to $host on port $port failed."
    }
    
    Write-Host "--------------------------"
}

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.jdbc.JdbcConnectionOptions;
import org.apache.flink.streaming.connectors.jdbc.JdbcSink;
import org.apache.flink.streaming.connectors.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class OracleJdbcSinkExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        JdbcConnectionOptions jdbcOptions = JdbcConnectionOptions.builder()
                .setUrl("jdbc:oracle:thin:@//localhost:1521/SID")
                .setUsername("username")
                .setPassword("password")
                .setDriverName("oracle.jdbc.driver.OracleDriver")
                .build();

        env.fromElements(
                Tuple2.of(1, "John"),
                Tuple2.of(2, "Alice"),
                Tuple2.of(3, "Bob"))
                .addSink(JdbcSink.sink(
                        "MERGE INTO mytable USING (SELECT ? AS id, ? AS name FROM dual) src " +
                                "ON (mytable.id = src.id) " +
                                "WHEN MATCHED THEN UPDATE SET mytable.name = src.name " +
                                "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (src.id, src.name)",
                        new OracleJdbcStatementBuilder(),
                        jdbcOptions));

        env.execute("Oracle JdbcSink Example");
    }

    public static class OracleJdbcStatementBuilder implements JdbcStatementBuilder<Tuple2<Integer, String>> {
        @Override
        public void accept(PreparedStatement statement, Tuple2<Integer, String> record) throws SQLException {
            statement.setInt(1, record.f0);
            statement.setString(2, record.f1);
        }
    }
}





import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ReadFields;
import org.apache.flink.api.java.io.CsvReaderFormat;
import org.apache.flink.api.java.io.FileInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.FileSource;
import org.apache.flink.streaming.api.functions.source.FileSource.FileSourceBuilder;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class CsvReaderExample {
  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    CsvReaderFormat<Tuple2<Integer, String>> format = new CsvReaderFormat<>(
        Tuple2.class,
        CsvReaderFormat.DEFAULT_FIELD_DELIMITER,
        CsvReaderFormat.DEFAULT_LINE_DELIMITER,
        CsvReaderFormat.DEFAULT_IGNORE_FIRST_LINE
    );

    FileSourceBuilder<Tuple2<Integer, String>, FileSource<Tuple2<Integer, String>>> fileSourceBuilder =
        FileSource.forRecordFormat(new Path("/path/to/csv/file"), format);

    FileInputFormat.setInputFilePath(fileSourceBuilder, new Path("/path/to/csv/file"));
    FileInputFormat.setFilesFilter(fileSourceBuilder, FileInputFormat.createDefaultFilter());
    fileSourceBuilder.setMonitoringFile("");

    FileSource<Tuple2<Integer, String>> fileSource = fileSourceBuilder.build();

    DataSet<Tuple2<Integer, String>> data = env.createInput(fileSource);

    DataSet<String> result = data.map(new MapFunction<Tuple2<Integer, String>, String>() {
        @Override
        public String map(Tuple2<Integer, String> value) throws Exception {
            return value.f1;
        }
    });

    result.print();
  }
}


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.FileSource;
import org.apache.flink.streaming.api.functions.source.FileSource.FileSourceBuilder;
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit;
import org.apache.flink.streaming.api.functions.source.TimestampedInputSplitAssigner;
import org.apache.flink.streaming.api.functions.source.TimestampedInputSplitAssigner.Context;
import org.apache.flink.streaming.api.functions.source.TimestampedInputSplitAssigner.Provider;
import org.apache.flink.streaming.api.functions.source.assigners.FileSplitAssigner;
import org.apache.flink.streaming.api.functions.source.assigners.SerialFileProcessingMode;
import org.apache.flink.streaming.api.functions.source.assigners.SimpleVersionedSerialization;
import org.apache.flink.streaming.api.functions.source.assigners.TimestampedFilesSplitter;
import org.apache.flink.streaming.api.functions.source.assigners.TimestampedFilesSplitter.TimestampedFiles;
import org.apache.flink.streaming.api.watermark.Watermark;

public class S3CSVReader {

    public static void main(String[] args) throws Exception {
        
        // create the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // configure S3 input
        String s3Path = "s3://your-bucket-name/your-csv-folder/";
        Path filePath = new Path(s3Path);
        FileInputFormat<String> inputFormat = new TextInputFormat(filePath);
        
        // read CSV files continuously
        FileSourceBuilder<String> builder = FileSource.forRecordStreamFormat(inputFormat);
        builder.monitorContinuously(FileProcessingMode.PROCESS_CONTINUOUSLY, 1000);
        builder.setFilePath(filePath);
        builder.setDeserializationSchema(inputFormat);
        builder.setFilesFilter(FileInputFormat.DEFAULT_FILTER);
        
        DataStream<String> inputStream = env.addSource(builder.build());
        
        // parse CSV data into tuples and group by key
        DataStream<Tuple2<String, Integer>> counts = inputStream
            .map(new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String line) throws Exception {
                    String[] fields = line.split(",");
                    String key = fields[0];
                    int value = Integer.parseInt(fields[1]);
                    return new Tuple2<>(key, value);
                }
            })
            .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                @Override
                public String getKey(Tuple2<String, Integer> tuple) throws Exception {
                    return tuple.f0;
                }
            })
            .sum(1);
        
        // print the results
        counts.print();
        
        // execute the job
        env.execute("S3 CSV Reader");
    }
}



// Set up the execution environment
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Define the source
DataStream<Tuple4<Integer, String, Integer, String>> myTable = env
    .readFile(new TextInputFormat(new Path("/path/to/csv/file.csv")), "/path/to/csv/file.csv")
    .map(new MapFunction<String, Tuple4<Integer, String, Integer, String>>() {
        @Override
        public Tuple4<Integer, String, Integer, String> map(String value) throws Exception {
            String[] tokens = value.split(",");
            return new Tuple4<Integer, String, Integer, String>(
                    Integer.parseInt(tokens[0]),
                    tokens[1],
                    Integer.parseInt(tokens[2]),
                    tokens[3]
            );
        }
    });

// Define the schema for the table
TableSchema schema = TableSchema.builder()
    .field("id", DataTypes.INT())
    .field("name", DataTypes.STRING())
    .field("age", DataTypes.INT())
    .field("city", DataTypes.STRING())
    .build();

// Convert the DataStream to a Table
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
Table table = tableEnv.fromDataStream(myTable, schema);

// Print the result
tableEnv.toRetractStream(table, Row.class).print();

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
