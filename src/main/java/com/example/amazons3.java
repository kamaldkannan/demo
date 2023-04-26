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




import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.table.sources.ParquetTableSource
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.Row

val s3AccessKey = "yourS3AccessKey"
val s3SecretKey = "yourS3SecretKey"
val s3Bucket = "yourS3BucketName"
val s3ObjectKey = "yourS3ObjectKey"  // e.g. "path/to/file.csv"

// Define the schema of your CSV file
val fieldNames = arrayOf("id", "name", "age")
val fieldTypes = arrayOf<LogicalType>(
    DataTypes.INT().getLogicalType(),
    DataTypes.STRING().getLogicalType(),
    DataTypes.INT().getLogicalType()
)
val rowType = RowType(fieldTypes, fieldNames)

// Create a CsvTableSource with the schema and S3 file location
val csvSource = CsvTableSource.builder()
    .path("s3a://$s3AccessKey:$s3SecretKey@$s3Bucket/$s3ObjectKey")
    .ignoreFirstLine()
    .fieldDelimiter(",")
    .field("id", Types.INT())
    .field("name", Types.STRING())
    .field("age", Types.INT())
    .build()

// Create an ExecutionEnvironment (or StreamExecutionEnvironment for streaming)
val env = ExecutionEnvironment.getExecutionEnvironment()

// Create a TableEnvironment
val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
val tableEnv = TableEnvironment.create(settings)

// Register the CSV table source with the TableEnvironment
tableEnv.registerTableSource("my_table", csvSource)

// Execute a SQL query on the CSV table
val result = tableEnv.sqlQuery("SELECT * FROM my_table WHERE age > 30")

// Convert the result to a DataSet (or DataStream for streaming)
val typeInfo: TypeInformation<Row> = RowTypeInfo(csvSource.fieldTypes, csvSource.fieldNames)
val resultSet = tableEnv.toDataSet<Row>(result, typeInfo)

// Print the result to the console
resultSet.print()
