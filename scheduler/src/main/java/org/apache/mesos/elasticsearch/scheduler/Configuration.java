package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;

/**
 * Holder object for framework configuration.
 */
public class Configuration {

    private static final double CPUS = 0.2;

    private static final double MEM = 512;

    private static final double DISK = 250;

    private String zookeeperHost;

    private int numberOfHwNodes;

    private State state;

    private String version;

    private int managementApiPort;

    public static double getCpus() {
        return CPUS;
    }

    public static double getMem() {
        return MEM;
    }

    public static double getDisk() {
        return DISK;
    }

    public State getState() {
        return state;
    }

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

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setManagementApiPort(int managementApiPort) {
        this.managementApiPort = managementApiPort;
    }

    public int getManagementApiPort() {
        return managementApiPort;
    }
}
