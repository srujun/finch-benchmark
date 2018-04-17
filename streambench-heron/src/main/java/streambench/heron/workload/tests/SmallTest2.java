package streambench.heron.workload.tests;

import com.twitter.heron.streamlet.*;
import streambench.heron.KafkaSink;
import streambench.heron.KafkaSource;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class SmallTest2 {

    public static void main(String[] args) throws IOException {
        List<String> bootstrapServers = Arrays.asList("ip-172-31-5-82:9092", "ip-172-31-6-235:9092");

        Random rand = new Random();

        Builder builder = Builder.newBuilder();
        System.out.println("Setting up Heron Streamlets...");

        Streamlet<KeyValue<String, String>> source =
                builder.newSource(new KafkaSource(bootstrapServers, "source1"))
                       .setName("source1");

        source
            .reduceByKeyAndWindow(
                KeyValue::getKey,
                WindowConfig.SlidingTimeWindow(Duration.ofSeconds(1), Duration.ofSeconds(1)),
                "",
                (currValue, newMsg) -> newMsg.getValue())
            .map(
                windowKeyValue -> KeyValue.create(windowKeyValue.getKey().getKey(), windowKeyValue.getValue()))
            .toSink(new KafkaSink(bootstrapServers, "sink2"));

        Config config = Config.defaultConfig();

        new Runner().run("SmallTest2", config, builder);
    }
}
