package streambench;

import joptsimple.OptionSet;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunnerMain;
import org.apache.samza.runtime.ApplicationRunnerOperation;
import org.apache.samza.util.Util;
import streambench.workload.WorkloadParser;

import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BenchmarkApplicationMain extends ApplicationRunnerMain {

    private static final String WORKLOAD_CONFIG_KEY = "streambench.workload.path";

    public static void main(String[] args) throws Exception {
        /* Parse the command-line options */
        ApplicationRunnerCommandLine cmdLine = new ApplicationRunnerCommandLine();
        OptionSet options = cmdLine.parser().parse(args);
        Config orgConfig = cmdLine.loadConfig(options);
        Config config = Util.rewriteConfig(orgConfig);
        ApplicationRunnerOperation op = cmdLine.getOperation(options);

        /* Get the workload files */
        if(!config.containsKey(WORKLOAD_CONFIG_KEY))
            throw new SamzaException(WORKLOAD_CONFIG_KEY + " not specified");
        String workloadPath = config.get(WORKLOAD_CONFIG_KEY);
        URI workloadURI = new URI(workloadPath);
        File workloadFile = new File(workloadURI.getPath());

        if(!workloadFile.exists())
            throw new SamzaException(workloadPath + " does not exist");

        // if(!config.containsKey(STREAM_APPLICATION_CLASS_CONFIG))
        //     throw new SamzaException("Samza " + STREAM_APPLICATION_CLASS_CONFIG + " not defined");

        /* Parse the workload file */
        Map<String, String> workloadOptions = WorkloadParser.getWorkloadOptions(new FileReader(workloadFile));

        /* Combine the stored config and the workload options */
        List<Map<String, String>> allConfigs = new ArrayList<>();
        allConfigs.add(config);
        allConfigs.add(workloadOptions);
        MapConfig finalConfig = new MapConfig(allConfigs);
        finalConfig.forEach(
            (k, v) -> System.out.println("Final Config: " + k + "=" + v)
        );

        System.out.println("Network:");
        System.out.println(WorkloadParser.getWorkloadAsNetwork(new FileReader(workloadFile)).toString());

        ApplicationRunner runner = ApplicationRunner.fromConfig(finalConfig);
        // StreamApplication app = (StreamApplication) Class.forName(config.get(STREAM_APPLICATION_CLASS_CONFIG)).newInstance();
        BenchmarkApplication app = new BenchmarkApplication();
        switch (op) {
            case RUN:
                runner.run(app);
                break;
            case KILL:
                runner.kill(app);
                break;
            case STATUS:
                System.out.println(runner.status(app));
                break;
            default:
                throw new IllegalArgumentException("Unrecognized operation: " + op);
        }
    }
}
