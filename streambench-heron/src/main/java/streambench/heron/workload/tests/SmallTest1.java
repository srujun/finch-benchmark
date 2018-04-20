package streambench.heron.workload.tests;

import com.twitter.heron.streamlet.*;
import streambench.heron.KafkaSink;
import streambench.heron.KafkaSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class SmallTest1 {

    public static void main(String[] args) throws IOException {
        List<String> bootstrapServers = Arrays.asList("ip-172-31-5-82:9092", "ip-172-31-6-235:9092");

        Random rand = new Random();

        Builder builder = Builder.newBuilder();
        System.out.println("Setting up Heron Streamlets...");

        Streamlet<KeyValue<String, String>> source =
                builder.newSource(new KafkaSource(bootstrapServers, "source1"))
                       .setName("source1");

        source
            .setNumPartitions(4)
            .filter(msg -> (rand.nextDouble() <= 0.5))
            .flatMap(msg -> Arrays.asList(msg, msg))
            .toSink(new KafkaSink(bootstrapServers, "sink1"));

//        Config config = Config.defaultConfig();
        Config config = Config.newBuilder()
                .setNumContainers(8)
                .setPerContainerRamInMegabytes(512)
                .setSerializer(Config.Serializer.KRYO)
                .setDeliverySemantics(Config.DeliverySemantics.ATLEAST_ONCE)
                .build();

        new Runner().run("SmallTest1", config, builder);
    }
}
