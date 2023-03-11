@RestController
@RequestMapping("/api")
@Api(value = "Kafka Offset Seeker API")
public class KafkaOffsetSeeker {

    @Autowired
    private KafkaConsumer<String, String> kafkaConsumer;

    @ApiOperation(value = "Get the offset for a given consumer group and partition.")
    @GetMapping("/offset/{group}/{topic}/{partition}")
    public ResponseEntity<Long> getOffset(
            @PathVariable String group,
            @PathVariable String topic,
            @PathVariable int partition
    ) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);

        try {
            // Assign the topic partition to the consumer
            kafkaConsumer.assign(Collections.singletonList(topicPartition));

            // Seek to the beginning of the partition
            kafkaConsumer.seekToBeginning(Collections.singletonList(topicPartition));

            // Poll the partition to get the current offset
            kafkaConsumer.poll(Duration.ofMillis(100));
            long currentOffset = kafkaConsumer.position(topicPartition);

            // Seek back to the end of the partition
            kafkaConsumer.seekToEnd(Collections.singletonList(topicPartition));

            // Poll the partition to get the end offset
            kafkaConsumer.poll(Duration.ofMillis(100));
            long endOffset = kafkaConsumer.position(topicPartition);

            // Seek back to the current offset
            kafkaConsumer.seek(topicPartition, currentOffset);

            return ResponseEntity.ok(endOffset - currentOffset);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @ApiOperation(value = "Check if the API is running.")
    @GetMapping("/health")
    public ResponseEntity<String> healthCheck() {
        return ResponseEntity.ok("API is running.");
    }

    @ApiOperation(value = "Get the API version.")
    @GetMapping("/version")
    public ResponseEntity<String> getVersion() {
        return ResponseEntity.ok("1.0.0");
    }
}
