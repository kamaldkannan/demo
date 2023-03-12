import java.util.*;
import javax.annotation.PostConstruct;
import javax.net.ssl.SSLException;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.web.bind.annotation.*;

@SpringBootApplication
@RestController
@RequestMapping("/api")
public class KafkaOffsetSeeker {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.group-id}")
    private String groupId;

    @Value("${kafka.ssl.truststore-location}")
    private String truststoreLocation;

    @Value("${kafka.ssl.truststore-password}")
    private String truststorePassword;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private KafkaConsumer<String, Object> consumer;

    @PostConstruct
    private void initConsumer() throws SSLException {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // SSL configuration
        props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
        props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
        props.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

        // Set up deserializer with error handling
        JsonDeserializer<Object> deserializer = new JsonDeserializer<>(Object.class, false);
        deserializer.setRemoveTypeHeaders(false);
        deserializer.addTrustedPackages("*");
        ErrorHandlingDeserializer<Object> errorHandlingDeserializer = new ErrorHandlingDeserializer<>(deserializer);

        consumer = new KafkaConsumer<>(props, new StringDeserializer(), errorHandlingDeserializer);
    }

    @GetMapping("/offsets")
    public ResponseEntity<String> seekOffsets(@RequestBody Map<String, Long> partitionOffsets) {
        try {
            String topic = "my-topic";
            consumer.subscribe(Collections.singletonList(topic));

            // Poll for records to ensure consumer is assigned partitions
            consumer.poll(Duration.ofSeconds(5));

            // Seek to specified partition offsets
            for (Map.Entry<String, Long> entry : partitionOffsets.entrySet()) {
                String[] parts = entry.getKey().split("-");
                int partition = Integer.parseInt(parts[1]);
                long offset = entry.getValue();
                consumer.seek(new TopicPartition(topic, partition), offset);
                System.out.printf("Group: %s, Topic: %s, Partition: %d, Offset: %d\n",
                        groupId, topic, partition, offset);
            }

            return ResponseEntity.ok("Offsets seeked successfully.");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error seeking offsets: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaOffsetSeeker.class, args);
    }
}
