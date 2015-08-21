package org.apache.mesos.elasticsearch.scheduler;

import com.beust.jcommander.JCommander;
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
    public static final String CLUSTER_NAME = "/mesos-ha";

    public Configuration(String[] args) {
        final JCommander jCommander = new JCommander(this);
        try {
            jCommander.parse(args); // Parse command line args into configuration class.
        } catch (com.beust.jcommander.ParameterException ex) {
            System.out.println(ex);
            jCommander.setProgramName("(Options preceded by an asterisk are required)");
            jCommander.usage();
            throw ex;
        }
    }

    // **** ZOOKEEPER
    @Parameter(names = {"--zookeeperTimeout"}, description = "The timeout for connecting to zookeeper.")
    private long zookeeperTimeout = 20000L;

    public long getZookeeperTimeout() {
        return zookeeperTimeout;
    }

    public static final String ZOOKEEPER_URL = "--zookeeperUrl";
    @Parameter(names = {"-zk", ZOOKEEPER_URL}, required = true, description = "Zookeeper urls in the format zk://IP:PORT,IP:PORT,...)")
    private String zookeeperUrl = "zk://mesos.master:2181";

    private String getZookeeperUrl() {
        return zookeeperUrl;
    }


    // **** ELASTICSEARCH
    @Parameter(names = {"--elasticsearchCpu"}, description = "The amount of CPU resource to allocate to the elasticsearch instance.")
    private double cpus = 1.0;

    public double getCpus() {
        return cpus;
    }

    // Todo (pnw): Remove ram parameter
    public static final String ELASTICSEARCH_RAM = "--elasticsearchRam";
    @Parameter(names = {"-ram", ELASTICSEARCH_RAM}, description = "The amount of ram resource to allocate to the elasticsearch instance.")
    private double mem = 256;

    public double getMem() {
        return mem;
    }

    @Parameter(names = {"--elasticsearchDisk"}, description = "The amount of Disk resource to allocate to the elasticsearch instance.")
    private double disk = 1024;

    public double getDisk() {
        return disk;
    }

    @Parameter(names = {"-n", "--numberOfElasticsearchNodes"}, description = "Number of elasticsearch instances.")
    private int numberOfElasticsearchNodes = 3;

    public int getNumberOfElasticsearchNodes() {
        return numberOfElasticsearchNodes;
    }


    // **** WEB UI
    // Todo (pnw): Remove m parameter
    @Parameter(names = {"-m", "--webUiPort"}, description = "TCP port for web ui interface.")
    private int webUiPort = 31100; // Default is more likely to work on a default Mesos installation

    public int getWebUiPort() {
        return webUiPort;
    }


    // **** FRAMEWORK
    private String version = "0.3.0";

    public String getVersion() {
        return version;
    }

    @Parameter(names = {"--frameworkName"}, description = "The name given to the framework.")
    private String frameworkName = "elasticsearch";

    public String getFrameworkName() {
        return frameworkName;
    }

    @Parameter(names = {"--executorName"}, description = "The name given to the executor task.")
    private String executorName = "elasticsearch-executor";
    public String getTaskName() {
        return executorName;
    }

    // DCOS Certification requirement 01
    @Parameter(names = {"--frameworkFailoverTimeout"}, description = "The time before Mesos kills a scheduler and tasks if it has not recovered.")
    private double frameworkFailoverTimeout = 2592000; // Mesos will kill framework after 1 month if marathon does not restart.
    public double getFailoverTimeout() {
        return frameworkFailoverTimeout;
    }


    // ****************** Runtime configuration **********************
    private SerializableState state;

    public Protos.FrameworkID getFrameworkId() {
        return getFrameworkState().getFrameworkID();
    }

    public void setFrameworkState(FrameworkState frameworkState) {
        this.frameworkState = frameworkState;
    }

    private FrameworkState frameworkState;

    public FrameworkState getFrameworkState() {
        if (frameworkState == null) {
            frameworkState = new FrameworkState(getState());
        }
        return frameworkState;
    }


    // ******* Helper methods
    public SerializableState getState() {
        if (state == null) {
            org.apache.mesos.state.State zkState = new ZooKeeperState(
                    getMesosStateZKURL(),
                    getZookeeperTimeout(),
                    TimeUnit.MILLISECONDS,
                    "/" + getFrameworkName() + CLUSTER_NAME);
            state = new SerializableZookeeperState(zkState);
        }
        return state;
    }

    public String getMesosStateZKURL() {
        ZKFormatter mesosStateZKFormatter = new MesosStateZKFormatter(new ZKAddressParser());
        return mesosStateZKFormatter.format(getZookeeperUrl());
    }

    public String getMesosZKURL() {
        ZKFormatter mesosZKFormatter = new MesosZKFormatter(new ZKAddressParser());
        return mesosZKFormatter.format(getZookeeperUrl());
    }
}
