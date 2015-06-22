package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;

/**
 * Holder object for framework configuration.
 */
public class Configuration {

    public static final double CPUS = 0.2;

    public static final double MEM = 512;

    public static final double DISK = 250;

    private String zookeeperHost;

    private int numberOfHwNodes;

    private State state;

    public void setState(State state) {
        this.state = state;
    }

    public int getNumberOfHwNodes() {
        return numberOfHwNodes;
    }

    public void setNumberOfHwNodes(int numberOfHwNodes) {
        this.numberOfHwNodes = numberOfHwNodes;
    }

    public String getZookeeperHost() {
        return zookeeperHost;
    }

    public void setZookeeperHost(String zookeeperHost) {
        this.zookeeperHost = zookeeperHost;
    }

    public int getZookeeperPort() {
        return 2181;
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
}
