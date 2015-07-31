package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;

/**
 * Holder object for framework configuration.
 */
public class Configuration {

    private double cpus = 0.2;

    private double mem = 256;

    private double disk = 250;

    private int numberOfHwNodes;

    private State state;

    private String version;

    private String zookeeperUrl;

    private int managementApiPort;

    public double getCpus() {
        return cpus;
    }

    public double getMem() {
        return mem;
    }

    public void setMem(double newMem) {
        mem = newMem;
    }

    public double getDisk() {
        return disk;
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

    public void setZookeeperUrl(String zookeeperUrl) {
        this.zookeeperUrl = zookeeperUrl;
    }

    public String getZookeeperUrl() {
        return zookeeperUrl;
    }

    public void setManagementApiPort(int managementApiPort) {
        this.managementApiPort = managementApiPort;
    }

    public int getManagementApiPort() {
        return managementApiPort;
    }
}
