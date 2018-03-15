package streambench;

import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.WorkloadParser;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;


public class BenchmarkApplication implements StreamApplication {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkApplication.class);

    private static final String WORKLOAD_FILE_KEY = "streambench.workload.path";

    // InfluxDB
    private static final String INFLUXDB_IP_KEY = "streambench.workload.influxdb";
    private static final String INFLUXDB_DATABASE = "streambench-metrics";
    private static final String INFLUXDB_RP = "streambench-retention-policy";
    // static InfluxDB influxDB;

    // Metrics
    private static final String METRICS_STREAM_ID = "metrics";
    private static final String[] METRICS_HEADERS = {"job-id", "job-name", "host", "container-name", "source"};
    private static final Set<String> METRICS_HEADERS_SET = new HashSet<>(Arrays.asList(METRICS_HEADERS));

    @Override
    public void init(StreamGraph graph, Config config) {
        String workloadFilePath;
        try {
            workloadFilePath = new URI(config.get(WORKLOAD_FILE_KEY)).getPath();
        } catch (URISyntaxException e) {
            logger.error("Invalid workload path");
            throw new SamzaException(e);
        }

        // setup the workload streams
        WorkloadParser.setupStreams(graph, workloadFilePath);

        /* TODO: METRICS COLLECTION USING INFLUXDB JAVA CLIENT
        influxDB = InfluxDBFactory.connect(config.get(INFLUXDB_IP_KEY), "root", "root");
        influxDB.enableBatch(50, 5, TimeUnit.SECONDS);
        if(!influxDB.databaseExists(INFLUXDB_DATABASE)) {
            influxDB.createDatabase(INFLUXDB_DATABASE);
        }
        influxDB.setDatabase(INFLUXDB_DATABASE);

        // setup the metrics collection
        MessageStream<MetricsSnapshot> metrics = graph.getInputStream(METRICS_STREAM_ID);
        metrics.sink((msg, collector, coordinator) -> {
            long timestamp = msg.header().time();

            // extract the header
            Map<String, Object> tags = new HashMap<>(msg.header().getAsMap());
            tags.keySet().retainAll(METRICS_HEADERS_SET);

            msg.metrics().getAsMap().forEach((group, metric) -> {
                Point.Builder pointBuilder = Point.measurement(group);
                metric.forEach((key, value) -> pointBuilder.addField(key, (double) value));
                tags.forEach((tagName, tagValue) -> pointBuilder.tag(tagName, tagValue.toString()));

                logger.info(pointBuilder.build().toString());
                influxDB.write(pointBuilder.build());
            });
        });
        */
    }

}
