package streambench.heron;

import com.twitter.heron.streamlet.Context;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.Sink;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaSink implements Sink<KeyValue<String, String>> {

    private KafkaProducer<String, String> producer;
    private AdminClient kafkaClient;

    private Collection<String> bootstrapServers;
    private String topic;

    public KafkaSink(Collection<String> bootstrapServers, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
    }

    @Override
    public void setup(Context context) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "1"); // only wait for ack from Kafka leader
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        kafkaClient = AdminClient.create(props);
        try {
            Set<String> existingTopics = kafkaClient.listTopics().names().get();
            NewTopic newTopic = new NewTopic(topic, 5, (short) 1);

            int retries = 5;
            while(!existingTopics.contains(topic) && retries > 0) {
                System.out.println("Trying to create topic " + topic + "...");
                retries -= 1;
                kafkaClient.createTopics(Collections.singletonList(newTopic)).all().get();

                /* wait for a second before retrieving topics again */
                Thread.sleep(1000);
                existingTopics = kafkaClient.listTopics().names().get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        producer = new KafkaProducer<>(props);
    }

    @Override
    public void put(KeyValue<String, String> tuple) {
        final ProducerRecord<String, String> record = new ProducerRecord<>(topic, tuple.getKey(), tuple.getValue());
        producer.send(record);
    }

    @Override
    public void cleanup() {
        producer.close();
    }
}
