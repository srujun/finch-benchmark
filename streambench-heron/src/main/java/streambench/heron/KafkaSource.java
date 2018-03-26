package streambench.heron;

import com.twitter.heron.streamlet.Context;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.Source;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

public class KafkaSource implements Source<KeyValue<String, String>> {

    private KafkaConsumer<String, String> kafkaConsumer;

    private Collection<String> bootstrapServers;
    private String topic;

    public KafkaSource(Collection<String> bootstrapServers, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
    }

    @Override
    public void setup(Context context) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "streambench");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public Collection<KeyValue<String, String>> get() {
        Collection<KeyValue<String, String>> messages = new ArrayList<>();

        kafkaConsumer.poll(100).forEach(
            keyValuePair -> messages.add(new KeyValue<>(keyValuePair.key(), keyValuePair.value()))
        );

        return messages;
    }

    @Override
    public void cleanup() {
        kafkaConsumer.close();
    }
}
