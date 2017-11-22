package streambench.workload.pojo;

import java.util.Map;

public class WorkloadConfig {

    private Map<String, WorkloadSource> sources;
    private Map<String, WorkloadTransformation> transformations;

    public Map<String, WorkloadSource> getSources() {
        return sources;
    }

    public void setSources(Map<String, WorkloadSource> sources) {
        this.sources = sources;
    }

    public Map<String, WorkloadTransformation> getTransformations() {
        return transformations;
    }

    public void setTransformations(Map<String, WorkloadTransformation> transformations) {
        this.transformations = transformations;
    }
}
