package streambench.workload;

import com.google.gson.Gson;
import streambench.system.BenchmarkMessageFactory;
import streambench.workload.pojo.WorkloadConfig;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class WorkloadParser {

    public static Map<String, String> getWorklaodOptions(FileReader workloadFile) {
        Map<String, String> options = new HashMap<>();

        Gson gson = new Gson();
        WorkloadConfig workloadConfig = gson.fromJson(workloadFile, WorkloadConfig.class);

        workloadConfig.getSources().forEach(
            (name, src) -> {
                System.out.println("keydist=" + src.getKey_dist_params());
                String key;
                String systemName = name + "-system";

                key = "systems." + systemName + ".samza.factory";
                options.put(key, BenchmarkMessageFactory.class.getCanonicalName());

                key = "streams." + name + ".samza.system";
                options.put(key, systemName);

                key = "streams." + name + ".samza.key.serde";
                options.put(key, "string");
                key = "streams." + name + ".samza.msg.serde";
                options.put(key, "string");
            }
        );

        workloadConfig.getTransformations().forEach(
            (name, transformation) -> {
                System.out.println("Transformation name: " + name);
                System.out.println("Transformation operator: " + transformation.getOperator());
                System.out.println("Transformation input: " + transformation.getInput());
            }
        );

        return options;
    }
}
