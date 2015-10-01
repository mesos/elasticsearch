package org.apache.mesos.elasticsearch.scheduler;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.validators.CLIValidators;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.IpPortsListZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.MesosZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.ZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;

/**
 * Holder object for framework configuration.
 */
@SuppressWarnings("PMD.TooManyFields")
public class Configuration {
    // **** ELASTICSEARCH
    public static final String ELASTICSEARCH_CPU = "--elasticsearchCpu";
    public static final String ELASTICSEARCH_RAM = "--elasticsearchRam";
    public static final String ELASTICSEARCH_DISK = "--elasticsearchDisk";
    // **** WEB UI
    public static final String WEB_UI_PORT = "--webUiPort";
    public static final String FRAMEWORK_NAME = "--frameworkName";
    public static final String EXECUTOR_NAME = "--executorName";
    public static final String DATA_DIR = "--dataDir";
    public static final String DEFAULT_HOST_DATA_DIR = "/var/lib/mesos/slave/elasticsearch";
    // DCOS Certification requirement 01
    public static final String FRAMEWORK_FAILOVER_TIMEOUT = "--frameworkFailoverTimeout";
    // DCOS Certification requirement 13
    public static final String FRAMEWORK_ROLE = "--frameworkRole";
    public static final String EXECUTOR_HEALTH_DELAY = "--executorHealthDelay";
    public static final String EXECUTOR_TIMEOUT = "--executorTimeout";
    public static final String EXECUTOR_IMAGE = "--executorImage";
    public static final String DEFAULT_EXECUTOR_IMAGE = "mesos/elasticsearch-executor";
    public static final String EXECUTOR_FORCE_PULL_IMAGE = "--executorForcePullImage";
    public static final String FRAMEWORK_PRINCIPAL = "--frameworkPrincipal";
    public static final String FRAMEWORK_SECRET_PATH = "--frameworkSecretPath";
    private static final Logger LOGGER = Logger.getLogger(Configuration.class);
    @Parameter(names = {EXECUTOR_HEALTH_DELAY}, description = "The delay between executor healthcheck requests (ms).", validateValueWith = CLIValidators.PositiveLong.class)
    private static Long executorHealthDelay = 30000L;
    // **** ZOOKEEPER
    private final ZookeeperCLIParameter zookeeperCLI = new ZookeeperCLIParameter();
    private final ElasticsearchCLIParameter elasticsearchCLI = new ElasticsearchCLIParameter();
    @Parameter(names = {ELASTICSEARCH_CPU}, description = "The amount of CPU resource to allocate to the elasticsearch instance.", validateValueWith = CLIValidators.PositiveDouble.class)
    private double cpus = 1.0;
    @Parameter(names = {ELASTICSEARCH_RAM}, description = "The amount of ram resource to allocate to the elasticsearch instance (MB).", validateValueWith = CLIValidators.PositiveDouble.class)
    private double mem = 256;
    @Parameter(names = {ELASTICSEARCH_DISK}, description = "The amount of Disk resource to allocate to the elasticsearch instance (MB).", validateValueWith = CLIValidators.PositiveDouble.class)
    private double disk = 1024;
    @Parameter(names = {WEB_UI_PORT}, description = "TCP port for web ui interface.", validateValueWith = CLIValidators.PositiveInteger.class)
    private int webUiPort = 31100; // Default is more likely to work on a default Mesos installation
    // **** FRAMEWORK
    private String version = "0.4.3";
    @Parameter(names = {FRAMEWORK_NAME}, description = "The name given to the framework.", validateWith = CLIValidators.NotEmptyString.class)
    private String frameworkName = "elasticsearch";
    @Parameter(names = {EXECUTOR_NAME}, description = "The name given to the executor task.", validateWith = CLIValidators.NotEmptyString.class)
    private String executorName = "elasticsearch-executor";
    @Parameter(names = {DATA_DIR}, description = "The data directory used by Docker volumes in the executors.")
    private String dataDir = DEFAULT_HOST_DATA_DIR;
    @Parameter(names = {FRAMEWORK_FAILOVER_TIMEOUT}, description = "The time before Mesos kills a scheduler and tasks if it has not recovered (ms).", validateValueWith = CLIValidators.PositiveDouble.class)
    private double frameworkFailoverTimeout = 2592000; // Mesos will kill framework after 1 month if marathon does not restart.
    @Parameter(names = {FRAMEWORK_ROLE}, description = "Used to group frameworks for allocation decisions, depending on the allocation policy being used.", validateWith = CLIValidators.NotEmptyString.class)
    private String frameworkRole = "*"; // This is the default if none is passed to Mesos
    @Parameter(names = {EXECUTOR_TIMEOUT},
            description = "The maximum executor healthcheck timeout (ms). Must be greater than " + EXECUTOR_HEALTH_DELAY + ". Will start new executor after this length of time.",
            validateValueWith = GreaterThanHealthDelay.class)
    private Long executorTimeout = 60000L;
    @Parameter(names = {EXECUTOR_IMAGE}, description = "The docker executor image to use.", validateWith = CLIValidators.NotEmptyString.class)
    private String executorImage = DEFAULT_EXECUTOR_IMAGE;
    @Parameter(names = {EXECUTOR_FORCE_PULL_IMAGE}, arity = 1, description = "Option to force pull the executor image.")
    private Boolean executorForcePullImage = false;
    @Parameter(names = {FRAMEWORK_PRINCIPAL}, description = "The principal to use when registering the framework (username).")
    private String frameworkPrincipal = "";
    @Parameter(names = {FRAMEWORK_SECRET_PATH}, description = "The path to the file which contains the secret for the principal (password). Password in file must not have a newline.")
    private String frameworkSecretPath = "";
    // ****************** Runtime configuration **********************

    public Configuration(String... args) {
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

    public double getCpus() {
        return cpus;
    }

    public double getMem() {
        return mem;
    }

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

    public int getWebUiPort() {
        return webUiPort;
    }

    public String getVersion() {
        return version;
    }

    public String getFrameworkName() {
        return frameworkName;
    }

    public String getTaskName() {
        return executorName;
    }

    public String getDataDir() {
        return dataDir;
    }

    public double getFailoverTimeout() {
        return frameworkFailoverTimeout;
    }

    public String getFrameworkRole() {
        return frameworkRole;
    }

    public Long getExecutorHealthDelay() {
        return executorHealthDelay;
    }

    public Long getExecutorTimeout() {
        return executorTimeout;
    }

    public String getExecutorImage() {
        return executorImage;
    }

    public Boolean getExecutorForcePullImage() {
        return executorForcePullImage;
    }

    // ******* Helper methods
    public String getMesosStateZKURL() {
        ZKFormatter mesosStateZKFormatter = new IpPortsListZKFormatter(new ZKAddressParser());
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
        ZKFormatter mesosZKFormatter = new IpPortsListZKFormatter(new ZKAddressParser());
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

    public ZookeeperCLIParameter getZookeeperCLI() {
        return zookeeperCLI;
    }

    public ElasticsearchCLIParameter getElasticsearchCLI() {
        return elasticsearchCLI;
    }

    public String getFrameworkSecretPath() {
        return frameworkSecretPath;
    }

    public String getFrameworkPrincipal() {
        return frameworkPrincipal;
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
