package streambench.workload;

import java.util.List;
import java.util.Map;

public class WorkloadConfig {

    private List<WorkloadSource> sources;
    private Map<String, WorkloadOperator> transformations;

    public List<WorkloadSource> getSources() {
        return sources;
    }

    public void setSources(List<WorkloadSource> sources) {
        this.sources = sources;
    }

    public Map<String, WorkloadOperator> getTransformations() {
        return transformations;
    }

    public void setTransformations(Map<String, WorkloadOperator> transformations) {
        this.transformations = transformations;
    }
}
