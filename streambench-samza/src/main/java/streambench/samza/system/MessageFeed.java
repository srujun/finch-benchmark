package streambench.samza.system;

import org.apache.commons.math3.distribution.AbstractIntegerDistribution;

public interface MessageFeed {
    void sendMessage(String key, String value);
    AbstractIntegerDistribution getKeyDist();
    AbstractIntegerDistribution getMsgLenDist();
    AbstractIntegerDistribution getRateDist();
}
