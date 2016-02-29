package org.apache.mesos.elasticsearch.scheduler;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.validators.CLIValidators;
import org.apache.mesos.elasticsearch.common.util.NetworkUtils;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.IpPortsListZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.MesosZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.ZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

/**
 * Holder object for framework configuration.
 */
@SuppressWarnings("PMD.TooManyFields")
public class Configuration {
    // **** ELASTICSEARCH
    public static final String ELASTICSEARCH_CPU = "--elasticsearchCpu";
    public static final String ELASTICSEARCH_RAM = "--elasticsearchRam";
    public static final String ELASTICSEARCH_DISK = "--elasticsearchDisk";
    public static final String ELASTICSEARCH_EXECUTOR_CPU = "--elasticsearchExecutorCpu";
    public static final String ELASTICSEARCH_EXECUTOR_RAM =  "--elasticsearchExecutorRam";
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
    public static final String EXECUTOR_TIMEOUT = "--executorTimeout";
    public static final String EXECUTOR_IMAGE = "--elasticsearchDockerImage";
    public static final String EXECUTOR_BINARY = "--elasticsearchBinaryUrl";
    public static final String DEFAULT_EXECUTOR_IMAGE = "elasticsearch:latest";
    public static final String EXECUTOR_FORCE_PULL_IMAGE = "--executorForcePullImage";
    public static final String FRAMEWORK_PRINCIPAL = "--frameworkPrincipal";
    public static final String FRAMEWORK_SECRET_PATH = "--frameworkSecretPath";
    public static final String ES_TAR = "public/elasticsearch.tar.gz";
    public static final String ES_BINARY = "./elasticsearch-*/bin/elasticsearch";
    private static final Logger LOGGER = Logger.getLogger(Configuration.class);
    public static final String FRAMEWORK_USE_DOCKER = "--frameworkUseDocker";
    public static final String JAVA_HOME = "--javaHome";
    public static final String USE_IP_ADDRESS = "--useIpAddress";
    public static final String ELASTICSEARCH_PORTS = "--elasticsearchPorts";
    public static final String CONTAINER_PATH_DATA = "/usr/share/elasticsearch/data";
    public static final String CONTAINER_PATH_CONF = "/usr/share/elasticsearch/config";
    public static final String HOST_SANDBOX = "./."; // Due to some protobuf weirdness. Requires './.' Not just '.'
    public static final String HOST_PATH_HOME = HOST_SANDBOX + "/es_home";
    public static final String HOST_PATH_CONF = HOST_SANDBOX;

    // **** ZOOKEEPER
    private final ZookeeperCLIParameter zookeeperCLI = new ZookeeperCLIParameter();
    private final ElasticsearchCLIParameter elasticsearchCLI = new ElasticsearchCLIParameter();
    @Parameter(names = {ELASTICSEARCH_CPU}, description = "The amount of CPU resource to allocate to the elasticsearch instance.", validateValueWith = CLIValidators.PositiveDouble.class)
    private double cpus = 1.0;
    @Parameter(names = {ELASTICSEARCH_RAM}, description = "The amount of ram resource to allocate to the elasticsearch instance (MB).", validateValueWith = CLIValidators.PositiveDouble.class)
    private double mem = 256;
    @Parameter(names = {ELASTICSEARCH_DISK}, description = "The amount of Disk resource to allocate to the elasticsearch instance (MB).", validateValueWith = CLIValidators.PositiveDouble.class)
    private double disk = 1024;
    @Parameter(names = {ELASTICSEARCH_EXECUTOR_CPU}, description = "The amount of CPU resource to allocate to the elasticsearch executor.", validateValueWith = CLIValidators.PositiveDouble.class)
    private double executorCpus = 0.1;
    @Parameter(names = {ELASTICSEARCH_EXECUTOR_RAM}, description = "The amount of ram resource to allocate to the elasticsearch executor (MB).", validateValueWith = CLIValidators.PositiveDouble.class)
    private double executorMem = 32;
    @Parameter(names = {WEB_UI_PORT}, description = "TCP port for web ui interface.", validateValueWith = CLIValidators.PositiveInteger.class)
    private int webUiPort = 31100; // Default is more likely to work on a default Mesos installation
    @Parameter(names = {ELASTICSEARCH_PORTS}, description = "User specified ES HTTP and transport ports. [NOT RECOMMENDED]", validateWith = CLIValidators.NumericListOfSizeTwo.class)
    private String elasticsearchPorts = ""; // Defaults to Mesos specified ports.

    // **** FRAMEWORK
    private String version = "0.7.2";
    @Parameter(names = {FRAMEWORK_NAME}, description = "The name given to the framework.", validateWith = CLIValidators.NotEmptyString.class)
    private String frameworkName = "elasticsearch";
    @Parameter(names = {EXECUTOR_NAME}, description = "The name given to the executor task.", validateWith = CLIValidators.NotEmptyString.class)
    private String executorName = "elasticsearch-executor";
    @Parameter(names = {DATA_DIR}, description = "The host data directory used by Docker volumes in the executors. [DOCKER MODE ONLY]")
    private String dataDir = DEFAULT_HOST_DATA_DIR;
    @Parameter(names = {FRAMEWORK_FAILOVER_TIMEOUT}, description = "The time before Mesos kills a scheduler and tasks if it has not recovered (ms).", validateValueWith = CLIValidators.PositiveDouble.class)
    private double frameworkFailoverTimeout = 2592000; // Mesos will kill framework after 1 month if marathon does not restart.
    @Parameter(names = {FRAMEWORK_ROLE}, description = "Used to group frameworks for allocation decisions, depending on the allocation policy being used.", validateWith = CLIValidators.NotEmptyString.class)
    private String frameworkRole = "*"; // This is the default if none is passed to Mesos
    @Parameter(names = {EXECUTOR_IMAGE}, description = "The elasticsearch docker image to use. E.g. 'elasticsearch:latest' [DOCKER MODE ONLY]", validateWith = CLIValidators.NotEmptyString.class)
    private String executorImage = DEFAULT_EXECUTOR_IMAGE;
    @Parameter(names = {EXECUTOR_BINARY}, description = "The elasticsearch binary to use (Must be tar.gz format). " +
            "E.g. 'https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.2.0/elasticsearch-2.2.0.tar.gz' [JAR MODE ONLY]", validateWith = CLIValidators.NotEmptyString.class)
    private String executorBinary = "";
    @Parameter(names = {EXECUTOR_FORCE_PULL_IMAGE}, arity = 1, description = "Option to force pull the executor image. [DOCKER MODE ONLY]")
    private Boolean executorForcePullImage = false;
    @Parameter(names = {FRAMEWORK_PRINCIPAL}, description = "The principal to use when registering the framework (username).")
    private String frameworkPrincipal = "";
    @Parameter(names = {FRAMEWORK_SECRET_PATH}, description = "The path to the file which contains the secret for the principal (password). Password in file must not have a newline.")
    private String frameworkSecretPath = "";
    @Parameter(names = {FRAMEWORK_USE_DOCKER}, arity = 1, description = "The framework will use docker if true, or jar files if false. If false, the user must ensure that the scheduler jar is available to all slaves.")
    private Boolean isFrameworkUseDocker = true;
    private InetSocketAddress frameworkFileServerAddress;
    @Parameter(names = {JAVA_HOME}, description = "When starting in jar mode, if java is not on the path, you can specify the path here. [JAR MODE ONLY]", validateWith = CLIValidators.NotEmptyString.class)
    private String javaHome = "";
    @Parameter(names = {USE_IP_ADDRESS}, arity = 1, description = "If true, the framework will resolve the local ip address. If false, it uses the hostname.")
    private Boolean isUseIpAddress = false;

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

    public double getExecutorCpus() {
        return executorCpus;
    }

    public double getExecutorMem() {
        return executorMem;
    }

    public int getElasticsearchNodes() {
        return elasticsearchCLI.getElasticsearchNodes();
    }

    public void setElasticsearchNodes(int numberOfNodes) throws IllegalArgumentException {
        elasticsearchCLI.setElasticsearchNodes(numberOfNodes);
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

    public String getExecutorImage() {
        return executorImage;
    }

    public Boolean getExecutorForcePullImage() {
        return executorForcePullImage;
    }

    public Boolean getIsUseIpAddress() {
        return isUseIpAddress;
    }

    public String getElasticsearchBinary() {
        return executorBinary;
    }

    // ******* Helper methods
    public String getMesosStateZKURL() {
        ZKFormatter mesosStateZKFormatter = new IpPortsListZKFormatter(new ZKAddressParser());
        return mesosStateZKFormatter.format(zookeeperCLI.getZookeeperMesosUrl());
    }

    public String getMesosZKURL() {
        ZKFormatter mesosZKFormatter = new MesosZKFormatter(new ZKAddressParser());
        return mesosZKFormatter.format(zookeeperCLI.getZookeeperMesosUrl());
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

    public Boolean isFrameworkUseDocker() {
        return isFrameworkUseDocker;
    }

    public String getFrameworkFileServerAddress() {
        String result = "";
        if (frameworkFileServerAddress != null) {
            return NetworkUtils.addressToString(frameworkFileServerAddress, getIsUseIpAddress());
        }
        return result;
    }

    public String webUiAddress() {
        return NetworkUtils.addressToString(NetworkUtils.hostSocket(getWebUiPort()), getIsUseIpAddress());
    }

    public void setFrameworkFileServerAddress(InetSocketAddress addr) {
        if (addr != null) {
            frameworkFileServerAddress = addr;
        } else {
            LOGGER.error("Could not set webserver address. Was null.");
        }
    }

    public String getJavaHome() {
        if (!javaHome.isEmpty()) {
            return javaHome.replaceAll("java$", "").replaceAll("/$", "") + "/";
        } else {
            return "";
        }
    }

    public List<Integer> getElasticsearchPorts() {
        if (elasticsearchPorts.isEmpty()) {
            return Collections.emptyList();
        }
        String[] portsRaw = elasticsearchPorts.replace(" ", "").split(",");
        ArrayList<Integer> portsList = new ArrayList<>(2);
        for (String port : portsRaw) {
            portsList.add(Integer.parseInt(port));
        }
        return portsList;
    }

    public String nativeCommand(List<String> arguments) {
        String folders = getDataDir() + " " + HOST_SANDBOX;
        String mkdir = "mkdir -p " + folders + "; ";
        String chown = "chown -R nobody:nogroup " + folders + "; ";
        return mkdir +
                chown +
                " su -s /bin/sh -c \""
                + Configuration.ES_BINARY
                + " "
                + arguments.stream().collect(Collectors.joining(" "))
                + "\" nobody";
    }

    public List<String> esArguments(ClusterState clusterState, Protos.DiscoveryInfo discoveryInfo, Protos.SlaveID slaveID) {
        List<String> args = new ArrayList<>();
        List<Protos.TaskInfo> taskList = clusterState.getTaskList();
        String hostAddress = "";
        if (taskList.size() > 0) {
            Protos.TaskInfo taskInfo = taskList.get(0);
            String taskId = taskInfo.getTaskId().getValue();
            InetSocketAddress transportAddress = clusterState.getGuiTaskList().get(taskId).getTransportAddress();
            hostAddress = NetworkUtils.addressToString(transportAddress, getIsUseIpAddress()).replace("http://", "");
        }
        addIfNotEmpty(args, "--default.discovery.zen.ping.unicast.hosts", hostAddress);
        args.add("--default.http.port=" + discoveryInfo.getPorts().getPorts(Discovery.CLIENT_PORT_INDEX).getNumber());
        args.add("--default.transport.tcp.port=" + discoveryInfo.getPorts().getPorts(Discovery.TRANSPORT_PORT_INDEX).getNumber());
        args.add("--default.cluster.name=" + getElasticsearchClusterName());
        args.add("--default.node.master=true");
        args.add("--default.node.data=true");
        args.add("--default.node.local=false");
        args.add("--default.index.number_of_replicas=0");
        args.add("--default.index.auto_expand_replicas=0-all");
        if (!isFrameworkUseDocker()) {
            String taskSpecificDataDir = taskSpecificHostDir(slaveID);
            args.add("--path.home=" + HOST_PATH_HOME); // Cannot be overidden
            args.add("--default.path.data=" + taskSpecificDataDir);
            args.add("--path.conf=" + HOST_PATH_CONF); // Cannot be overidden
        } else {
            args.add("--path.data=" + CONTAINER_PATH_DATA); // Cannot be overidden
        }
        args.add("--default.bootstrap.mlockall=true");
        args.add("--default.network.bind_host=0.0.0.0");
        args.add("--default.network.publish_host=_non_loopback:ipv4_");
        args.add("--default.gateway.recover_after_nodes=1");
        args.add("--default.gateway.expected_nodes=1");
        args.add("--default.indices.recovery.max_bytes_per_sec=100mb");
        args.add("--default.discovery.type=zen");
        args.add("--default.discovery.zen.fd.ping_timeout=30s");
        args.add("--default.discovery.zen.fd.ping_interval=1s");
        args.add("--default.discovery.zen.fd.ping_retries=30");
        args.add("--default.discovery.zen.ping.multicast.enabled=false");


        return args;
    }

    public String taskSpecificHostDir(Protos.SlaveID slaveID) {
        return getDataDir() + "/" + getElasticsearchClusterName() + "/" + slaveID.getValue();
    }

    private void addIfNotEmpty(List<String> args, String key, String value) {
        if (!value.isEmpty()) {
            args.addAll(asList(key, value));
        }
    }
}
