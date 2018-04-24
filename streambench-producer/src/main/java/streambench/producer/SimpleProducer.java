package streambench.producer;

import org.apache.commons.math3.distribution.AbstractIntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import streambench.workload.pojo.WorkloadConfig;
import streambench.workload.pojo.WorkloadSource;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimpleProducer {
    private static final String NUM_KEYS_PARAM = "num_keys";

    private static RandomStringGenerator generator = new RandomStringGenerator.Builder().withinRange('a', 'z').build();

    private KafkaProducer<String, String> producer;
    private AdminClient client;
    private ScheduledExecutorService executorService;

    SimpleProducer(List<String> kafkaBrokers, WorkloadConfig workloadConfig, int partitions,
                   ScheduledExecutorService executorService) {
        kafkaBrokers.forEach(server -> System.out.println("Bootstrap server: " + server));

        final Properties kafkaConfig = new Properties();
        kafkaConfig.put("bootstrap.servers", kafkaBrokers);
        kafkaConfig.put("acks", "1"); // only wait for ack from Kafka leader
        kafkaConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(kafkaConfig);
        this.client = AdminClient.create(kafkaConfig);

        this.executorService = executorService;

        /* create the source topics that this producer will write to */
        try {
            createSources(workloadConfig.getSources(), partitions);
        } catch (Exception e) {
            System.err.println("Failed to create Kafka source topics!");
            e.printStackTrace();
            System.exit(-2);
        }

        /* create single threaded executors for each src */
        workloadConfig.getSources().forEach(
            (srcName, src) -> {
                try {
                    createEmitterForSource(srcName, src);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    System.exit(-2);
                }
            }
        );
    }

    private void createSources(Map<String, WorkloadSource> sources, int partitions)
            throws InterruptedException, ExecutionException {
        /* delete any existing source topics that may exist */
        Set<String> existingTopics = client.listTopics().names().get();
        Set<String> toDelete = new HashSet<>(existingTopics);
        toDelete.retainAll(sources.keySet());

        System.out.print("Existing topics: ");
        existingTopics.forEach(topic -> System.out.print(topic + " "));
        System.out.println();
        System.out.print("To delete topics: ");
        toDelete.forEach(topic -> System.out.print(topic + " "));
        System.out.println();

        int retries = 5;
        while((existingTopics.containsAll(toDelete) && !toDelete.isEmpty()) && retries > 0) {
            System.out.println("Trying delete...");
            retries -= 1;
            client.deleteTopics(toDelete).all().get();

            /* wait for a couple seconds before retrieving topics again */
            Thread.sleep(2000);
            existingTopics = client.listTopics().names().get();
        }

        /* create the source topics with given partition count */
        System.out.print("Creating topics: ");
        sources.keySet().forEach(topic -> System.out.print(topic + " "));
        System.out.println();

        List<NewTopic> newTopics = sources.keySet().stream()
                .map(topicName -> new NewTopic(topicName, partitions, (short) 1))
                .collect(Collectors.toList());
        client.createTopics(newTopics).all().get();

        /* finally, list the topics */
        Collection<String> finalTopics = client.listTopics().names().get();
        System.out.print("Final topics: ");
        finalTopics.forEach(topic -> System.out.print(topic + " "));
        System.out.println();
    }

    void createEmitterForSource(String srcName, WorkloadSource src) throws ClassNotFoundException {
        /* get the various distributions */
        AbstractIntegerDistribution keyDist = SimpleProducer.getDistribution(src.getKey_dist(), src.getKey_dist_params());
        // AbstractIntegerDistribution rateDist = SimpleProducer.getDistribution(src.getRate_dist(), src.getRate_dist_params());
        AbstractIntegerDistribution msgDist = SimpleProducer.getDistribution(src.getMsg_dist(), src.getMsg_dist_params());

        /* create a list of keys */
        int numKeys;
        if(src.getKey_dist_params().containsKey(NUM_KEYS_PARAM)) {
            numKeys = ((Double) src.getKey_dist_params().get(NUM_KEYS_PARAM)).intValue();
        } else {
            int lower = ((Double) src.getKey_dist_params().get("lower")).intValue();
            int upper = ((Double) src.getKey_dist_params().get("upper")).intValue();
            numKeys = upper - lower + 1; // upper and lower are inclusive
        }

        String[] keys = IntStream.range(0, numKeys)
                                 .mapToObj(i -> "key" + i)
                                 .toArray(String[]::new);
        System.out.println("keys: " + Arrays.toString(keys));

        final Runnable msgEmitter = () -> {
            int keyIndex = keyDist.sample() - 1;  // adjust for returned range of [1, k]
            if(keyIndex < 0) {
                System.err.println("GENERATED NEGATIVE KEY INDEX!");
                keyIndex = 0;
            }
            String msg = System.nanoTime() + "," + generator.generate(msgDist.sample());

            final ProducerRecord<String, String> record = new ProducerRecord<>(srcName, keys[keyIndex], msg);
            producer.send(record);
        };

        /* TODO: change to variable rate instead of fixed rate */
        int rate = ((Double) src.getRate_dist_params().get("rate")).intValue();
        System.out.println("rate: " + rate);

        executorService.scheduleAtFixedRate(msgEmitter, 0, 1000000 / rate, TimeUnit.MICROSECONDS);
    }

    static AbstractIntegerDistribution getDistribution(String type, Map<String, Object> params) throws ClassNotFoundException {
        if(type.contains("Zipf")) {
            return new ZipfDistribution(((Double) params.get(NUM_KEYS_PARAM)).intValue(), (Double) params.get("exponent"));
        } else if(type.contains("Uniform")) {
            return new UniformIntegerDistribution(((Double) params.get("lower")).intValue(), ((Double) params.get("upper")).intValue());
        } else {
            throw new ClassNotFoundException("Could not instantiate distribution of type " + type);
        }
    }
}
