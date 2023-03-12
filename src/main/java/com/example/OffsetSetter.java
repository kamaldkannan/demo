import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import java.util.*;

public class OffsetSetter {
    public static void main(String[] args) {
        String topicName = "test-topic";
        String groupName = "test-group";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupName);
        props.put("enable.auto.commit", "false");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // subscribe to the topic
        consumer.subscribe(Arrays.asList(topicName));
        
        // wait for the consumer to be assigned to partitions
        while (consumer.assignment().isEmpty()) {
            consumer.poll(Duration.ofMillis(100));
        }

        // create a map of topic partitions and the corresponding offsets to set
        Map<TopicPartition, Long> partitionOffsets = new HashMap<>();
        partitionOffsets.put(new TopicPartition(topicName, 0), 100L);
        partitionOffsets.put(new TopicPartition(topicName, 1), 200L);
        
        // set the partition offsets
        for (TopicPartition partition : partitionOffsets.keySet()) {
            Long offset = partitionOffsets.get(partition);
            consumer.seek(partition, offset);
        }
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Offset: " + record.offset() + ", Key: " + record.key() + ", Value: " + record.value());
                    consumer.commitAsync(Collections.singletonMap(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1)));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
