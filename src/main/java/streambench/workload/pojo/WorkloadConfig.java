package streambench.workload.pojo;

import java.util.List;
import java.util.Map;

public class WorkloadConfig {

    private Map<String, WorkloadSource> sources;
    private Map<String, WorkloadTransformation> transformations;
    private List<String> sinks;

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

    public List<String> getSinks() {
        return sinks;
    }

    public void setSinks(List<String> sinks) {
        this.sinks = sinks;
    }
}
