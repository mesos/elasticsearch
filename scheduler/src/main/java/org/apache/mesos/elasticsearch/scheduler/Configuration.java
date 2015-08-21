package org.apache.mesos.elasticsearch.scheduler;

import com.beust.jcommander.Parameter;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.MesosStateZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.MesosZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.ZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.apache.mesos.elasticsearch.scheduler.state.SerializableState;
import org.apache.mesos.elasticsearch.scheduler.state.SerializableZookeeperState;
import org.apache.mesos.state.ZooKeeperState;

import java.util.concurrent.TimeUnit;

/**
 * Holder object for framework configuration.
 */
public class Configuration {
    public static final long ZK_TIMEOUT = 20000L;
    public static final String FRAMEWORK_NAME = "/elasticsearch-mesos";
    public static final String CLUSTER_NAME = "/mesos-ha";

    private double cpus = 0.2;

    // Todo (pnw): Remove ram parameter
    @Parameter(names = {"-ram", "--elasticsearchRam"}, description = "The amount of ram to allocate to the elasticsearch instance")
    private double mem = 256;

    private String memUnits = "MB";

    private double disk = 250;

    private String diskUnits = "GB";

    @Parameter(names = {"-n", "--numberOfNodes"}, description = "Number of elasticsearch instances.")
    private int numberOfHwNodes = 3;

    private SerializableState state;

    private String version;

    @Parameter(names = {"-zk", "--zookeeperUrl"}, required = true, description = "Zookeeper urls in the format zk://IP:PORT,IP:PORT,...)")
    private String zookeeperUrl;

    // Todo (pnw): Remove m parameter
    @Parameter(names = {"-m", "--webUiPort"}, description = "TCP port for web ui interface.")
    private int managementApiPort = 8080;
    private FrameworkState frameworkState;

    public double getCpus() {
        return cpus;
    }

    public double getMem() {
        return mem;
    }

    public String getMemUnits() {
        return memUnits;
    }

    public double getDisk() {
        return disk;
    }

    public String getDiskUnits() {
        return diskUnits;
    }

    public SerializableState getState() {
        if (state == null) {
            org.apache.mesos.state.State zkState = new ZooKeeperState(
                    getMesosStateZKURL(),
                    ZK_TIMEOUT,
                    TimeUnit.MILLISECONDS,
                    FRAMEWORK_NAME + CLUSTER_NAME);
            state = new SerializableZookeeperState(zkState);
        }
        return state;
    }

    public int getNumberOfHwNodes() {
        return numberOfHwNodes;
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

    public String getTaskName() {
        return "esdemo";
    }

    public String getVersion() {
        return version;
    }

    private String getZookeeperUrl() {
        return zookeeperUrl;
    }

    public int getManagementApiPort() {
        return managementApiPort;
    }

    public String getMesosStateZKURL() {
        ZKFormatter mesosStateZKFormatter = new MesosStateZKFormatter(new ZKAddressParser());
        return mesosStateZKFormatter.format(getZookeeperUrl());
    }

    public String getMesosZKURL() {
        ZKFormatter mesosZKFormatter = new MesosZKFormatter(new ZKAddressParser());
        return mesosZKFormatter.format(getZookeeperUrl());
    }

    // ****************** Runtime configuration **********************
    public Protos.FrameworkID getFrameworkId() {
        return getFrameworkState().getFrameworkID();
    }

    public void setFrameworkState(FrameworkState frameworkState) {
        this.frameworkState = frameworkState;
    }

    public FrameworkState getFrameworkState() {
        if (frameworkState == null) {
            frameworkState = new FrameworkState(getState());
        }
        return frameworkState;
    }
}
