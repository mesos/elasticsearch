package org.apache.mesos.elasticsearch.scheduler;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.validators.CLIValidators;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.ElasticsearchZKFormatter;
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
@SuppressWarnings("PMD.TooManyFields")
public class Configuration {
    private static final Logger LOGGER = Logger.getLogger(Configuration.class);
    // **** ZOOKEEPER
    private final ZookeeperCLIParameter zookeeperCLI = new ZookeeperCLIParameter();
    private final ElasticsearchCLIParameter elasticsearchCLI = new ElasticsearchCLIParameter();

    public Configuration(String[] args) {
        final JCommander jCommander = new JCommander();
        jCommander.addObject(zookeeperCLI);
        jCommander.addObject(elasticsearchCLI);
        jCommander.addObject(this);
        try {
            jCommander.parse(args); // Parse command line args into configuration class.
        } catch (com.beust.jcommander.ParameterException ex) {
            System.out.println(ex);
            jCommander.setProgramName("(Options preceded by an asterisk are required)");
            jCommander.usage();
            throw ex;
        }
    }


    // **** ELASTICSEARCH
    public static final String ELASTICSEARCH_CPU = "--elasticsearchCpu";
    @Parameter(names = {ELASTICSEARCH_CPU}, description = "The amount of CPU resource to allocate to the elasticsearch instance.", validateValueWith = CLIValidators.PositiveDouble.class)
    private double cpus = 1.0;
    public double getCpus() {
        return cpus;
    }

    public static final String ELASTICSEARCH_RAM = "--elasticsearchRam";
    @Parameter(names = {ELASTICSEARCH_RAM}, description = "The amount of ram resource to allocate to the elasticsearch instance (MB).", validateValueWith = CLIValidators.PositiveDouble.class)
    private double mem = 256;
    public double getMem() {
        return mem;
    }

    public static final String ELASTICSEARCH_DISK = "--elasticsearchDisk";
    @Parameter(names = {ELASTICSEARCH_DISK}, description = "The amount of Disk resource to allocate to the elasticsearch instance (MB).", validateValueWith = CLIValidators.PositiveDouble.class)
    private double disk = 1024;
    public double getDisk() {
        return disk;
    }

    public int getElasticsearchNodes() {
        return elasticsearchCLI.getElasticsearchNodes();
    }

    public String getElasticsearchSettingsLocation() {
        return elasticsearchCLI.getElasticsearchSettingsLocation();
    }

    public String getElasticsearchClusterName() {
        return elasticsearchCLI.getElasticsearchClusterName();
    }

    // **** WEB UI
    public static final String WEB_UI_PORT = "--webUiPort";
    @Parameter(names = {WEB_UI_PORT}, description = "TCP port for web ui interface.", validateValueWith = CLIValidators.PositiveInteger.class)
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
    @Parameter(names = {FRAMEWORK_NAME}, description = "The name given to the framework.", validateWith = CLIValidators.NotEmptyString.class)
    private String frameworkName = "elasticsearch";
    public String getFrameworkName() {
        return frameworkName;
    }

    public static final String EXECUTOR_NAME = "--executorName";
    @Parameter(names = {EXECUTOR_NAME}, description = "The name given to the executor task.", validateWith = CLIValidators.NotEmptyString.class)
    private String executorName = "elasticsearch-executor";
    public String getTaskName() {
        return executorName;
    }

    // DCOS Certification requirement 01
    public static final String FRAMEWORK_FAILOVER_TIMEOUT = "--frameworkFailoverTimeout";
    @Parameter(names = {FRAMEWORK_FAILOVER_TIMEOUT}, description = "The time before Mesos kills a scheduler and tasks if it has not recovered (ms).", validateValueWith = CLIValidators.PositiveDouble.class)
    private double frameworkFailoverTimeout = 2592000; // Mesos will kill framework after 1 month if marathon does not restart.
    public double getFailoverTimeout() {
        return frameworkFailoverTimeout;
    }

    public static final String EXECUTOR_HEALTH_DELAY = "--executorHealthDelay";
    @Parameter(names = {EXECUTOR_HEALTH_DELAY}, description = "The delay between executor healthcheck requests (ms).", validateValueWith = CLIValidators.PositiveLong.class)
    private static Long executorHealthDelay = 30000L;
    public Long getExecutorHealthDelay() {
        return executorHealthDelay;
    }

    public static final String EXECUTOR_TIMEOUT = "--executorTimeout";
    @Parameter(names = {EXECUTOR_TIMEOUT},
            description = "The maximum executor healthcheck timeout (ms). Must be greater than " + EXECUTOR_HEALTH_DELAY + ". Will start new executor after this length of time.",
            validateValueWith = GreaterThanHealthDelay.class)
    private Long executorTimeout = 60000L;
    public Long getExecutorTimeout() {
        return executorTimeout;
    }

    public static final String EXECUTOR_IMAGE = "--executorImage";
    public static final String DEFAULT_EXECUTOR_IMAGE = "mesos/elasticsearch-executor";
    @Parameter(names = {EXECUTOR_IMAGE}, description = "The docker executor image to use.", validateWith = CLIValidators.NotEmptyString.class)
    private String executorImage = DEFAULT_EXECUTOR_IMAGE;
    public String getExecutorImage() {
        return executorImage;
    }

    public static final String EXECUTOR_FORCE_PULL_IMAGE = "--executorForcePullImage";
    @Parameter(names = {EXECUTOR_FORCE_PULL_IMAGE}, arity = 1, description = "Option to force pull the executor image.")
    private Boolean executorForcePullImage = false;
    public Boolean getExecutorForcePullImage() {
        return executorForcePullImage;
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
                    zookeeperCLI.getZookeeperMesosTimeout(),
                    TimeUnit.MILLISECONDS,
                    "/" + getFrameworkName() + "/" + elasticsearchCLI.getElasticsearchClusterName());
            state = new SerializableZookeeperState(zkState);
        }
        return state;
    }

    public String getMesosStateZKURL() {
        ZKFormatter mesosStateZKFormatter = new MesosStateZKFormatter(new ZKAddressParser());
        if (StringUtils.isBlank(zookeeperCLI.getZookeeperFrameworkUrl())) {
            LOGGER.info("Zookeeper framework option is blank, using Zookeeper for Mesos: " + zookeeperCLI.getZookeeperMesosUrl());
            return mesosStateZKFormatter.format(zookeeperCLI.getZookeeperMesosUrl());
        } else {
            LOGGER.info("Zookeeper framework option : " + zookeeperCLI.getZookeeperFrameworkUrl());
            return mesosStateZKFormatter.format(zookeeperCLI.getZookeeperFrameworkUrl());
        }
    }

    public String getMesosZKURL() {
        ZKFormatter mesosZKFormatter = new MesosZKFormatter(new ZKAddressParser());
        return mesosZKFormatter.format(zookeeperCLI.getZookeeperMesosUrl());
    }

    public String getFrameworkZKURL() {
        ZKFormatter mesosZKFormatter = new ElasticsearchZKFormatter(new ZKAddressParser());
        if (StringUtils.isBlank(zookeeperCLI.getZookeeperFrameworkUrl())) {
            LOGGER.info("Zookeeper framework option is blank, using Zookeeper for Mesos: " + zookeeperCLI.getZookeeperMesosUrl());
            return mesosZKFormatter.format(zookeeperCLI.getZookeeperMesosUrl());
        } else {
            LOGGER.info("Zookeeper framework option : " + zookeeperCLI.getZookeeperFrameworkUrl());
            return mesosZKFormatter.format(zookeeperCLI.getZookeeperFrameworkUrl());
        }
    }

    public long getFrameworkZKTimeout() {
        return zookeeperCLI.getZookeeperFrameworkTimeout();
    }

    /**
     * Ensures that the number is > than the EXECUTOR_HEALTH_DELAY
     */
    public static class GreaterThanHealthDelay extends CLIValidators.PositiveLong {
        @Override
        public void validate(String name, Long value) throws ParameterException {
            if (notValid(value) || value <= Configuration.executorHealthDelay) {
                throw new ParameterException("Parameter " + name + " should be greater than " + EXECUTOR_HEALTH_DELAY + " (found " + value + ")");
            }
        }
    }
}
