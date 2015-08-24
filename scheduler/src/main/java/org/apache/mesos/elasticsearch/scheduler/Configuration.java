package org.apache.mesos.elasticsearch.scheduler;

import com.beust.jcommander.*;
import org.apache.log4j.Logger;
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
    private static final Logger LOGGER = Logger.getLogger(Configuration.class);
    public static final String EXECUTOR_IMAGE = "--executorImage";

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
    public static final String ZOOKEEPER_TIMEOUT = "--zookeeperTimeout";
    @Parameter(names = {ZOOKEEPER_TIMEOUT}, description = "The timeout for connecting to zookeeper (ms).", validateValueWith = PositiveLong.class)
    private long zookeeperTimeout = 20000L;
    public long getZookeeperTimeout() {
        return zookeeperTimeout;
    }

    public static final String ZOOKEEPER_URL = "--zookeeperUrl";
    @Parameter(names = {"-zk", ZOOKEEPER_URL}, required = true, description = "Zookeeper urls in the format zk://IP:PORT,IP:PORT,...)", validateWith = NotEmptyString.class)
    private String zookeeperUrl = "zk://mesos.master:2181";
    private String getZookeeperUrl() {
        return zookeeperUrl;
    }


    // **** ELASTICSEARCH
    public static final String ELASTICSEARCH_CPU = "--elasticsearchCpu";
    @Parameter(names = {ELASTICSEARCH_CPU}, description = "The amount of CPU resource to allocate to the elasticsearch instance.", validateValueWith = PositiveDouble.class)
    private double cpus = 1.0;
    public double getCpus() {
        return cpus;
    }

    // Todo (pnw): Remove ram parameter
    public static final String ELASTICSEARCH_RAM = "--elasticsearchRam";
    @Parameter(names = {"-ram", ELASTICSEARCH_RAM}, description = "The amount of ram resource to allocate to the elasticsearch instance (MB).", validateValueWith = PositiveDouble.class)
    private double mem = 256;
    public double getMem() {
        return mem;
    }

    public static final String ELASTICSEARCH_DISK = "--elasticsearchDisk";
    @Parameter(names = {ELASTICSEARCH_DISK}, description = "The amount of Disk resource to allocate to the elasticsearch instance (MB).", validateValueWith = PositiveDouble.class)
    private double disk = 1024;
    public double getDisk() {
        return disk;
    }

    public static final String ELASTICSEARCH_NODES = "--elasticsearchNodes";
    @Parameter(names = {"-n", ELASTICSEARCH_NODES}, description = "Number of elasticsearch instances.", validateValueWith = OddNumberOfNodes.class)
    private int elasticsearchNodes = 3;
    public int getElasticsearchNodes() {
        return elasticsearchNodes;
    }

    public static final String ELASTICSEARCH_CLUSTER_NAME = "--elasticsearchClusterName";
    @Parameter(names = {ELASTICSEARCH_CLUSTER_NAME}, description = "Name of the elasticsearch cluster", validateWith = NotEmptyString.class)
    private String elasticsearchClusterName = "mesos-ha";
    public String getElasticsearchClusterName() {
        return elasticsearchClusterName;
    }

    // **** WEB UI
    // Todo (pnw): Remove m parameter
    public static final String WEB_UI_PORT = "--webUiPort";
    @Parameter(names = {"-m", WEB_UI_PORT}, description = "TCP port for web ui interface.", validateValueWith = PositiveInteger.class)
    private int webUiPort = 31100; // Default is more likely to work on a default Mesos installation
    public int getWebUiPort() {
        return webUiPort;
    }


    // **** FRAMEWORK
    private String version = "0.3.RC0";
    public String getVersion() {
        return version;
    }

    public static final String FRAMEWORK_NAME = "--frameworkName";
    @Parameter(names = {FRAMEWORK_NAME}, description = "The name given to the framework.", validateWith = NotEmptyString.class)
    private String frameworkName = "elasticsearch";
    public String getFrameworkName() {
        return frameworkName;
    }

    public static final String EXECUTOR_NAME = "--executorName";
    @Parameter(names = {EXECUTOR_NAME}, description = "The name given to the executor task.", validateWith = NotEmptyString.class)
    private String executorName = "elasticsearch-executor";
    public String getTaskName() {
        return executorName;
    }

    // DCOS Certification requirement 01
    public static final String FRAMEWORK_FAILOVER_TIMEOUT = "--frameworkFailoverTimeout";
    @Parameter(names = {FRAMEWORK_FAILOVER_TIMEOUT}, description = "The time before Mesos kills a scheduler and tasks if it has not recovered (ms).", validateValueWith = PositiveDouble.class)
    private double frameworkFailoverTimeout = 2592000; // Mesos will kill framework after 1 month if marathon does not restart.
    public double getFailoverTimeout() {
        return frameworkFailoverTimeout;
    }

    public static final String EXECUTOR_HEALTH_DELAY = "--executorHealthDelay";
    @Parameter(names = {EXECUTOR_HEALTH_DELAY}, description = "The delay between executor healthcheck requests (ms).", validateValueWith = PositiveLong.class)
    private static Long executorHealthDelay = 30000L;
    public Long getExecutorHealthDelay() {
        return executorHealthDelay;
    }

    public static final String EXECUTOR_TIMEOUT = "--executorTimeout";
    @Parameter(names = {EXECUTOR_TIMEOUT}, description = "The maximum executor healthcheck timeout (ms). Must be greater than " + EXECUTOR_HEALTH_DELAY + ". Will start new executor after this length of time.", validateValueWith = GreaterThanHealthDelay.class)
    private Long executorTimeout = 60000L;
    public Long getExecutorTimeout() {
        return executorTimeout;
    }

    public static final String EXECUTOR_IMAGE = "--executorImage";
    @Parameter(names = {EXECUTOR_IMAGE}, description = "The docker executor image to use.", validateWith = NotEmptyString.class)
    private String executorImage = "mesos/elasticsearch-executor";
    public String getEexecutorImage() {
        return executorImage;
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
                    "/" + getFrameworkName() + "/" + getElasticsearchClusterName());
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

    /**
     * Abstract class to validate a number.
     * @param <T> A numeric type
     */
    public abstract static class PositiveValue<T> implements IValueValidator<T> {
        @Override
        public void validate(String name, T value) throws ParameterException {
            if (notValid(value)) {
                throw new ParameterException("Parameter " + name + " should be greater than zero (found " + value + ")");
            }
        }

        public abstract Boolean notValid(T value);
    }

    /**
     * Validates a positive number. For type Long
     */
    public static class PositiveLong extends PositiveValue<Long> {
        @Override
        public Boolean notValid(Long value) {
            return value <= 0;
        }
    }

    /**
     * Validates a positive number. For type Double
     */
    public static class PositiveDouble extends PositiveValue<Double> {
        @Override
        public Boolean notValid(Double value) {
            return value <= 0;
        }
    }

    /**
     * Validates a positive number. For type Integer
     */
    public static class PositiveInteger extends PositiveValue<Integer> {
        @Override
        public Boolean notValid(Integer value) {
            return value <= 0;
        }
    }

    /**
     * Adds a warning message if an even number is encountered
     */
    public static class OddNumberOfNodes extends PositiveInteger {
        @Override
        public Boolean notValid(Integer value) {
            if (value % 2 == 0) {
                LOGGER.warn("Setting number of ES nodes to an even number. Not recommended!");
            }
            return super.notValid(value);
        }
    }

    /**
     * Ensures that the number is > than the EXECUTOR_HEALTH_DELAY
     */
    public static class GreaterThanHealthDelay extends PositiveLong {
        @Override
        public void validate(String name, Long value) throws ParameterException {
            if (notValid(value) || value <= Configuration.executorHealthDelay) {
                throw new ParameterException("Parameter " + name + " should be greater than " + EXECUTOR_HEALTH_DELAY + " (found " + value + ")");
            }
        }
    }

    /**
     * Ensures that the string is not empty. Will strip spaces.
     */
    public static class NotEmptyString implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            if (value.replace(" ", "").isEmpty()) {
                throw new ParameterException("Parameter " + name + " cannot be empty");
            }
        }
    }
}
