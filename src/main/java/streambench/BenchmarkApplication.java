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

    /* load generation reference: https://caffinc.github.io/2016/03/cpu-load-generator/ */
    private static final String WORKLOAD_FILE_KEY = "streambench.workload.path";

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
    }

}
