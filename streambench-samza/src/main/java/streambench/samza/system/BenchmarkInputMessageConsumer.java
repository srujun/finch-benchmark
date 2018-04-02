package streambench.samza.system;

import org.apache.commons.math3.distribution.AbstractIntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.samza.workload.SamzaWorkloadParser;
import streambench.workload.pojo.WorkloadConfig;
import streambench.workload.pojo.WorkloadSource;

import java.io.FileReader;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class BenchmarkInputMessageConsumer extends BlockingEnvelopeMap implements MessageFeed {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkInputMessageConsumer.class);

    private static final String WORKLOAD_FILE_KEY = "streambench.workload.path";

    private final String[] keys;

    private String systemName;
    private String streamName;
    private Instant startTime;

    private ScheduledFuture<?> emitterHandle;
    private final ScheduledExecutorService schedulerService;
    private final WorkloadSource streamSrc;

    private AbstractIntegerDistribution keyDist;
    private AbstractIntegerDistribution msgLenDist;
//    private AbstractIntegerDistribution rateDist;
    private int rate;  // per second
    private int partitions;

    private MessageEmitter messageEmitter;

    private int messagesSent;

    public BenchmarkInputMessageConsumer(String systemName, Config config) throws Exception {
        String workloadFilePath = new URI(config.get(WORKLOAD_FILE_KEY)).getPath();
        WorkloadConfig workloadConfig = SamzaWorkloadParser.instance().getWorkloadConfig(new FileReader(workloadFilePath));

        this.systemName = systemName;
        this.streamName = systemName.substring(0, systemName.lastIndexOf("-system"));
        logger.info("System=" + systemName + " Stream=" + streamName);
        this.streamSrc = workloadConfig.getSources().get(this.streamName);

        int numKeys = ((Double) streamSrc.getKey_dist_params().get("num_keys")).intValue();
        keys = IntStream.range(0, numKeys)
                        .mapToObj(i -> "key" + i).toArray(String[]::new);
        logger.info("Keys length = " + keys.length);

        /* TODO: works only for ZipfDistribution */
        Constructor keyDistCtor = Class.forName(streamSrc.getKey_dist()).getConstructor(int.class, double.class);
        keyDist = (ZipfDistribution) keyDistCtor.newInstance(
                ((Double) streamSrc.getKey_dist_params().get("num_keys")).intValue(),
                streamSrc.getKey_dist_params().get("exponent")
        );

        /* TODO: works only for UniformIntegerDistribution */
        Constructor msgLenDistCtor = Class.forName(streamSrc.getMsg_dist()).getConstructor(int.class, int.class);
        msgLenDist = (UniformIntegerDistribution) msgLenDistCtor.newInstance(
                ((Double) streamSrc.getMsg_dist_params().get("lower")).intValue(),
                ((Double) streamSrc.getMsg_dist_params().get("upper")).intValue()
        );

//        /* TODO: works only for UniformIntegerDistribution */
//        Constructor rateDistCtor = Class.forName(streamSrc.getRate_dist()).getConstructor(int.class, int.class);
//        keyDist = (UniformIntegerDistribution) rateDistCtor.newInstance(
//                streamSrc.getRate_dist_params().get("lower"), streamSrc.getRate_dist_params().get("upper"));
        rate = ((Double) streamSrc.getRate_dist_params().get("rate")).intValue();

        partitions = Integer.parseInt(config.get("job.container.count", "1"));

        this.messageEmitter = new MessageEmitter(this);
        this.schedulerService = Executors.newSingleThreadScheduledExecutor();
    }

    class MessageEmitter implements Runnable {
        private MessageFeed feed;
        private RandomStringGenerator generator;

        MessageEmitter(MessageFeed feedConsumer) {
            this.feed = feedConsumer;
            this.generator = new RandomStringGenerator.Builder().withinRange('a', 'z').build();
            logger.info("MessageEmitter created!");
        }

        @Override
        public void run() {
            int keyIndex = feed.getKeyDist().sample() - 1;
            int msgLen = feed.getMsgLenDist().sample();
            feed.sendMessage(keys[keyIndex], generator.generate(msgLen));
        }
    }

    @Override
    public void start() {
        logger.info("Starting BenchmarkInputMessageConsumer...");
        startTime = Instant.now();
        emitterHandle = schedulerService.scheduleAtFixedRate(messageEmitter,
                0, 1000 / rate, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        logger.info("Stopping BenchmarkInputMessageConsumer...");
        Duration totTime = Duration.between(startTime, Instant.now());
        logger.info("Sent " + messagesSent + " messages in " + totTime.getSeconds() + " seconds");
        emitterHandle.cancel(true);
        // schedulerService.shutdown();
    }

    @Override
    public void sendMessage(String key, String value) {
        logger.info(String.format("sendMessage called with [%s, %s]", key, value));

        SystemStreamPartition partition = new SystemStreamPartition(systemName, streamName, new Partition(key.hashCode() % partitions));
        IncomingMessageEnvelope msgEnvelope = new IncomingMessageEnvelope(partition, null, key, value);

        try {
            put(partition, msgEnvelope);
        } catch (InterruptedException e) {
            logger.warn("FAILED TO SEND MESSAGE");
            logger.error(e.toString());
        } finally {
            messagesSent++;
        }
    }

    @Override
    public AbstractIntegerDistribution getKeyDist() {
        return keyDist;
    }

    @Override
    public AbstractIntegerDistribution getMsgLenDist() {
        return msgLenDist;
    }

    @Override
    public AbstractIntegerDistribution getRateDist() {
        return null;
    }
}
