package streambench.samza.system;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkMessageFactory implements SystemFactory {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkMessageFactory.class);

    @Override
    public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry metricsRegistry) {
        logger.info("Getting BenchmarkMessageFactory consumer");
//        throw new SamzaException("Cannot consume from BenchmarkMessage system");

        SystemConsumer benchmarkSystemConsumer;

        try {
            benchmarkSystemConsumer = new BenchmarkInputMessageConsumer(systemName, config);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Could not create BenchmarkInputMessageConsumer: " + e.getMessage());
            for(StackTraceElement ste : e.getStackTrace())
                logger.error(ste.toString());

            throw new SamzaException("Error creating BenchmarkInputMessageConsumer");
        }

        return benchmarkSystemConsumer;
    }

    @Override
    public SystemProducer getProducer(String s, Config config, MetricsRegistry metricsRegistry) {
        logger.info("Getting BenchmarkMessageFactory producer");
        throw new SamzaException("Cannot produce to BenchmarkMessage system");
    }

    @Override
    public SystemAdmin getAdmin(String s, Config config) {
        int numContainers = Integer.parseInt(config.get("job.container.count", "1"));
        return new NPartitionsWithoutOffsetsSystemAdmin(numContainers);
    }
}
