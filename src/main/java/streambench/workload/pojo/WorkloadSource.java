package streambench.workload.pojo;

import java.util.Map;

public class WorkloadSource extends WorkloadNode {

    private String key_dist;
    private Map<String, Object> key_dist_params;

    private String msg_dist;
    private Map<String, Object> msg_dist_params;

    private String rate_dist;
    private Map<String, Object> rate_dist_params;

    public String getKey_dist() {
        return key_dist;
    }

    public void setKey_dist(String key_dist) {
        this.key_dist = key_dist;
    }

    public Map<String, Object> getKey_dist_params() {
        return key_dist_params;
    }

    public void setKey_dist_params(Map<String, Object> key_dist_params) {
        this.key_dist_params = key_dist_params;
    }

    public String getMsg_dist() {
        return msg_dist;
    }

    public void setMsg_dist(String msg_dist) {
        this.msg_dist = msg_dist;
    }

    public Map<String, Object> getMsg_dist_params() {
        return msg_dist_params;
    }

    public void setMsg_dist_params(Map<String, Object> msg_dist_params) {
        this.msg_dist_params = msg_dist_params;
    }

    public String getRate_dist() {
        return rate_dist;
    }

    public void setRate_dist(String rate_dist) {
        this.rate_dist = rate_dist;
    }

    public Map<String, Object> getRate_dist_params() {
        return rate_dist_params;
    }

    public void setRate_dist_params(Map<String, Object> rate_dist_params) {
        this.rate_dist_params = rate_dist_params;
    }
}
