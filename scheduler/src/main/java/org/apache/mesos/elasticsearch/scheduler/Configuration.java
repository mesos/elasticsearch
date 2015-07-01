package org.apache.mesos.elasticsearch.scheduler;

import org.apache.commons.lang.StringUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.zookeeper.model.ZKAddress;

import java.util.Iterator;
import java.util.List;

/**
 * Holder object for framework configuration.
 */
public class Configuration {

    public static final double CPUS = 0.2;

    public static final double MEM = 512;

    public static final double DISK = 250;

    private int numberOfHwNodes;

    private State state;

    private String version;

    private List<ZKAddress> zookeeperAddresses;

    private String zookeeperUrl;

    public void setState(State state) {
        this.state = state;
    }

    public int getNumberOfHwNodes() {
        return numberOfHwNodes;
    }

    public void setNumberOfHwNodes(int numberOfHwNodes) {
        this.numberOfHwNodes = numberOfHwNodes;
    }

    public String getFrameworkName() {
        return "elasticsearch";
    }

    // DCOS Certification requirement 01
    // The time before Mesos kills a scheduler and tasks if it has not recovered.
    // Mesos will kill framework after 1 month if marathon does not restart.
    public double getFailoverTimeout() {
        return 2592000;
    }

    public Protos.FrameworkID getFrameworkId() {
        return state.getFrameworkID();
    }

    public void setFrameworkId(Protos.FrameworkID frameworkId) {
        this.state.setFrameworkId(frameworkId);
    }

    public String getTaskName() {
        return "esdemo";
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setZookeeperAddresses(List<ZKAddress> zookeeperAddresses) {
        this.zookeeperAddresses = zookeeperAddresses;
    }

    public void setZookeeperUrl(String zookeeperUrl) {
        this.zookeeperUrl = zookeeperUrl;
    }

    public String getZookeeperUrl() {
        return zookeeperUrl;
    }

    public String getZookeeperServers() {
        Iterator<String> hostPorts = zookeeperAddresses.stream().map(zk -> zk.getAddress() + ":" + zk.getPort()).iterator();
        return StringUtils.join(hostPorts, ",");
    }
}
