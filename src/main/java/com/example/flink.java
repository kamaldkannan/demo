// import the necessary classes
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Schema;

// create a TableEnvironment
EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
TableEnvironment tableEnv = TableEnvironment.create(settings);

// create a descriptor for the S3 file system
FileSystem s3 = new FileSystem();
s3.path("s3://my-bucket/my-file.csv");
s3.property("aws.access.key.id", "my-access-key-id");
s3.property("aws.secret.access.key", "my-secret-access-key");
s3.property("region", "us-east-1");

// create a descriptor for the CSV format
FormatDescriptor format = new FormatDescriptor();
format.type("csv")
      .fieldDelimiter(",")
      .field("field1", "STRING")
      .field("field2", "INT")
      .field("field3", "DOUBLE");

// create a schema for the table
Schema schema = new Schema();
schema.field("field1", "STRING")
      .field("field2", "INT")
      .field("field3", "DOUBLE");

// register the file system, format, and schema with the TableEnvironment
tableEnv
    .connect(s3)
    .withFormat(format)
    .withSchema(schema)
    .createTemporaryTable("myTable");

// query the table
tableEnv.sqlQuery("SELECT * FROM myTable").execute().print();
