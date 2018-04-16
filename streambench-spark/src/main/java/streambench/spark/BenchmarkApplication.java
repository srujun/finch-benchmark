package streambench.spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

public class BenchmarkApplication {

    private static final String KAFKA_BROKERS = "ip-172-31-23-57:9092:ip-172-31-27-22:9092";
    private static final Set<String> KAFKA_TOPICS_SET = new HashSet<>(Collections.singletonList("source1"));

    public static void main(String[] args) throws Exception {
        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf();
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", KAFKA_BROKERS);

        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put("bootstrap.servers", KAFKA_BROKERS);
        producerConfig.put("key.serializer", StringSerializer.class);
        producerConfig.put("value.serializer", StringSerializer.class);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
            jssc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.Subscribe(KAFKA_TOPICS_SET, kafkaParams)
        );

        Random rand = new Random();

        JavaDStream<ConsumerRecord<String, String>> filtered = messages.filter(msg -> rand.nextBoolean());
        JavaDStream<ConsumerRecord<String, String>> mapped = filtered.flatMap(msg -> Arrays.asList(msg, msg).iterator());

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}
