package streambench.workload;

import com.google.gson.Gson;
import streambench.system.BenchmarkMessageFactory;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class WorkloadParser {

    public static Map<String, String> getWorklaodOptions(FileReader workloadFile) {
        Map<String, String> options = new HashMap<>();

        Gson gson = new Gson();
        WorkloadConfig workloadConfig = gson.fromJson(workloadFile, WorkloadConfig.class);

        for(WorkloadSource src : workloadConfig.getSources()) {
            String key = "systems." + src.getName() + ".samza.factory";
            options.put(key, BenchmarkMessageFactory.class.getCanonicalName());

            key = "streams." + src.getName() + ".samza.key.serde";
            options.put(key, "string");


        }

        workloadConfig.getTransformations().forEach(
            (name, operator) -> {
                System.out.println("Transformation name: " + name);
                System.out.println("Transformation operator: " + operator.getOperator());
                System.out.println("Transformation input: " + operator.getInput());
            }
        );

        return options;
    }
}
