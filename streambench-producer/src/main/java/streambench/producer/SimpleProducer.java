package streambench.producer;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.distribution.AbstractIntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import streambench.workload.WorkloadParser;
import streambench.workload.pojo.WorkloadConfig;
import streambench.workload.pojo.WorkloadSource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimpleProducer {

    private static final String WORKLOAD_OPT = "workload-path";
    private static final String PROPERTIES_OPT = "properties-path";

    private static final String NUM_KEYS_PARAM = "num_keys";
    private static final String KAFKA_SERVERS_PARAM = "systems.kafka.producer.bootstrap.servers";
    private static final String JOB_CONTAINER_COUNT = "job.container.count";

    private static RandomStringGenerator generator = new RandomStringGenerator.Builder().withinRange('a', 'z').build();

    public static void main(String[] args) {

        WorkloadConfig workloadConfig = null;
        Config propertiesConfig = null;
        try {
            Pair<WorkloadConfig, Config> pair = SimpleProducer.getWorkloadConfigFromArgs(args);
            workloadConfig = pair.getLeft();
            propertiesConfig = pair.getRight();
        } catch (Exception e) {
            System.exit(-1);
        }

        List<String> bootstrapServers =
                propertiesConfig.getList(KAFKA_SERVERS_PARAM, Collections.singletonList("localhost:9092"));
        bootstrapServers.forEach(server -> System.out.println("Bootstrap server: " + server));

        int containerCount = propertiesConfig.getInt(JOB_CONTAINER_COUNT, 1);

        final Properties kafkaConfig = new Properties();
        kafkaConfig.put("bootstrap.servers", bootstrapServers);
        kafkaConfig.put("acks", "1"); // only wait for ack from Kafka leader
        kafkaConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        final KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfig);
        final AdminClient client = AdminClient.create(kafkaConfig);

        /* create the source topics that this producer will write to */
        try {
            SimpleProducer.createSources(client, workloadConfig.getSources(), containerCount);
        } catch (Exception e) {
            System.err.println("Failed to create Kafka source topics!");
            e.printStackTrace();
            System.exit(-2);
        }

        /* create executor thread pool of size = num of topics to write to */
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(workloadConfig.getSources().size());

        /* create single threaded executors for each src */
        workloadConfig.getSources().forEach(
            (srcName, src) -> {
                try {
                    SimpleProducer.createEmitterForSource(producer, executorService, srcName, src);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    System.exit(-2);
                }
            }
        );
    }

    private static void createSources(AdminClient client, Map<String, WorkloadSource> sources, int partitions)
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

        client.deleteTopics(toDelete).all().get();

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

    private static void createEmitterForSource(KafkaProducer<String, String> producer,
                                               ScheduledExecutorService executorService,
                                               String srcName, WorkloadSource src) throws ClassNotFoundException {
        /* get the various distributions */
        AbstractIntegerDistribution keyDist = SimpleProducer.getDistribution(src.getKey_dist(), src.getKey_dist_params());
        // AbstractIntegerDistribution rateDist = SimpleProducer.getDistribution(src.getRate_dist(), src.getRate_dist_params());
        AbstractIntegerDistribution msgDist = SimpleProducer.getDistribution(src.getMsg_dist(), src.getMsg_dist_params());

        /* create a list of keys */
        int numKeys = ((Double) src.getKey_dist_params().get(NUM_KEYS_PARAM)).intValue();
        String[] keys = IntStream.range(0, numKeys)
                                 .mapToObj(i -> "key" + i)
                                 .toArray(String[]::new);

        final Runnable msgEmitter = () -> {
            int keyIndex = keyDist.sample() - 1;  // adjust for returned range of [1, k]
            String msg = generator.generate(msgDist.sample());

            final ProducerRecord<String, String> record = new ProducerRecord<>(srcName, keys[keyIndex], msg);
            producer.send(record);
        };

        /* TODO: change to variable rate instead of fixed rate */
        int rate = ((Double) src.getRate_dist_params().get("rate")).intValue();

        executorService.scheduleAtFixedRate(msgEmitter, 0, 1000 / rate, TimeUnit.MILLISECONDS);
    }

    private static Pair<WorkloadConfig, Config> getWorkloadConfigFromArgs(String[] fromArgs) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<File> workloadOpt = parser.accepts(WORKLOAD_OPT, "path to workload JSON file")
                .withRequiredArg()
                .ofType(File.class)
                .required();
        OptionSpec<File> propertiesOpt = parser.accepts(PROPERTIES_OPT, "path to Samza .properties file")
                .withRequiredArg()
                .ofType(File.class)
                .required();
        OptionSpec<Void> helpOpt = parser.acceptsAll(Arrays.asList("h", "help"), "Show help")
                .forHelp();

        OptionSet options = parser.parse(fromArgs);
        if(options.has(helpOpt) || (!options.has(workloadOpt) || !options.has(propertiesOpt))) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        FileReader workloadReader;
        FileReader propertiesReader;

        try {
            workloadReader = new FileReader(options.valueOf(workloadOpt));
        } catch (FileNotFoundException e) {
            System.err.println("File " + options.valueOf(workloadOpt).getPath() + " not found!");
            throw e;
        }
        try {
            propertiesReader = new FileReader(options.valueOf(propertiesOpt));
        } catch (FileNotFoundException e) {
            System.err.println("File " + options.valueOf(propertiesOpt).getPath() + " not found!");
            throw e;
        }

        Properties properties = new Properties();
        properties.load(propertiesReader);
        HashMap propertiesMap = new HashMap<>(properties);
        MapConfig propertiesConfig = new MapConfig(propertiesMap);

        return new ImmutablePair<>(WorkloadParser.getWorkloadConfig(workloadReader), propertiesConfig);
    }

    private static AbstractIntegerDistribution getDistribution(String type, Map<String, Object> params) throws ClassNotFoundException {
        if(type.contains("Zipf")) {
            return new ZipfDistribution(((Double) params.get("num_keys")).intValue(), (Double) params.get("exponent"));
        } else if(type.contains("Uniform")) {
            return new UniformIntegerDistribution(((Double) params.get("lower")).intValue(), ((Double) params.get("upper")).intValue());
        } else {
            throw new ClassNotFoundException("Could not instantiate distribution of type " + type);
        }
    }
}
