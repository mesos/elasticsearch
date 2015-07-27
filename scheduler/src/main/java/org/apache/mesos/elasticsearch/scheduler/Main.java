package org.apache.mesos.elasticsearch.scheduler;

import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.MesosStateZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.MesosZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.ZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;
import org.apache.mesos.elasticsearch.scheduler.configuration.ExecutorEnvironmentalVariables;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Application which starts the Elasticsearch scheduler
 */
public class Main {

    public static final String NUMBER_OF_HARDWARE_NODES = "n";

    public static final String ZK_URL = "zk";

    public static final String MANAGEMENT_API_PORT = "m";
    public static final String RAM = "ram";

    private Options options;

    private Configuration configuration;

    public Main() {
        this.options = new Options();
        this.options.addOption(NUMBER_OF_HARDWARE_NODES, "numHardwareNodes", true, "number of hardware nodes");
        this.options.addOption(ZK_URL, "zookeeperUrl", true, "Zookeeper urls in the format zk://IP:PORT,IP:PORT,...)");
        this.options.addOption(MANAGEMENT_API_PORT, "StatusPort", true, "TCP port for status interface. Default is 8080");
        this.options.addOption(RAM, "ElasticsearchRam", true, "Amount of RAM to give the Elasticsearch instances");
    }

    public static void main(String[] args) {
        Main main = new Main();
        main.run(args);
    }

    public void run(String[] args) {
        checkEnv();
        checkHostConfig();
        
        try {
            parseCommandlineOptions(args);
        } catch (ParseException | IllegalArgumentException e) {
            printUsageAndExit();
            return;
        }

        final ElasticsearchScheduler scheduler = new ElasticsearchScheduler(configuration, new TaskInfoFactory());

        HashMap<String, Object> properties = new HashMap<>();
        properties.put("server.port", String.valueOf(configuration.getManagementApiPort()));
        new SpringApplicationBuilder(WebApplication.class)
                .properties(properties)
                .initializers(applicationContext -> applicationContext.getBeanFactory().registerSingleton("scheduler", scheduler))
                .initializers(applicationContext -> applicationContext.getBeanFactory().registerSingleton("configuration", configuration))
                .showBanner(false)
                .run(args);

        scheduler.run();
    }

    private void checkHostConfig() {
        String ethConfig;
        try {
            Runtime r = Runtime.getRuntime();
            Process p = r.exec("ifconfig docker");
            p.waitFor();
            ethConfig = IOUtils.toString(p.getInputStream());
        } catch (Exception ex) {
            throw new IllegalArgumentException("Unable to read machine ifconfig");
        }
        if (ethConfig.isEmpty()) {
            throw new IllegalArgumentException("Docker network mode is not HOST. Please run with --net=host.");
        }
    }

    private void checkEnv() {
        Map<String, String> env = System.getenv();
        checkHeap(env.get(ExecutorEnvironmentalVariables.JAVA_OPTS));
    }

    private void checkHeap(String s) {
        if (s == null || s.isEmpty()) {
            throw new IllegalArgumentException("Scheduler heap space not set!");
        }
    }

    private void parseCommandlineOptions(String[] args) throws ParseException, IllegalArgumentException {
        configuration = new Configuration();

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String numberOfHwNodesString = cmd.getOptionValue(NUMBER_OF_HARDWARE_NODES);
        String zkUrl = cmd.getOptionValue(ZK_URL);
        String ram = cmd.getOptionValue(RAM, Double.toString(configuration.getMem()));
        String managementApiPort = cmd.getOptionValue(MANAGEMENT_API_PORT, "8080");


        if (numberOfHwNodesString == null || zkUrl == null) {
            printUsageAndExit();
            return;
        }

        configuration.setZookeeperUrl(getMesosZKURL(zkUrl));
        configuration.setVersion(getClass().getPackage().getImplementationVersion());
        configuration.setNumberOfHwNodes(Integer.parseInt(numberOfHwNodesString));
        configuration.setState(new State(new ZooKeeperStateInterfaceImpl(getMesosStateZKURL(zkUrl))));
        configuration.setMem(Double.parseDouble(ram));
        configuration.setManagementApiPort(Integer.parseInt(managementApiPort));
    }

    private String getMesosStateZKURL(String zkUrl) {
        ZKFormatter mesosStateZKFormatter = new MesosStateZKFormatter(new ZKAddressParser());
        return mesosStateZKFormatter.format(zkUrl);
    }

    private String getMesosZKURL(String zkUrl) {
        ZKFormatter mesosZKFormatter = new MesosZKFormatter(new ZKAddressParser());
        return mesosZKFormatter.format(zkUrl);
    }

    private void printUsageAndExit() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(configuration.getFrameworkName(), options);
        System.exit(2);
    }

}
