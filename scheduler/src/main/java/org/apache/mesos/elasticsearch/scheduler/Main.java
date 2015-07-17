package org.apache.mesos.elasticsearch.scheduler;

import org.apache.commons.cli.*;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.MesosStateZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.ZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.util.HashMap;

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
        this.options.addOption(ZK_URL, "Zookeeper URL", true, "Zookeeper urls zk://IP:PORT,IP:PORT,IP:PORT/mesos)");
        this.options.addOption(MANAGEMENT_API_PORT, "StatusPort", true, "TCP port for status interface. Default is 8080");
        this.options.addOption(RAM, "ElasticsearchRam", true, "Amount of RAM to give the Elasticsearch instances");
    }

    public static void main(String[] args) {
        Main main = new Main();
        main.run(args);
    }

    public void run(String[] args) {
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

    private void parseCommandlineOptions(String[] args) throws ParseException, IllegalArgumentException {
        configuration = new Configuration();

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        String numberOfHwNodesString = cmd.getOptionValue(NUMBER_OF_HARDWARE_NODES);
        String zkUrl = cmd.getOptionValue(ZK_URL);
        String ram = cmd.getOptionValue(RAM, Double.toString(configuration.getMem()));
        String managementApiPort = cmd.getOptionValue(MANAGEMENT_API_PORT, "8080");


        if (numberOfHwNodesString == null || zkUrl == null) {
            printUsageAndExit();
            return;
        }

        ZKFormatter zkFormatter = new MesosStateZKFormatter(new ZKAddressParser());
        String mesosStateZkUrl = zkFormatter.format(zkUrl);

        configuration.setZookeeperUrl(zkUrl);
        configuration.setVersion(getClass().getPackage().getImplementationVersion());
        configuration.setNumberOfHwNodes(Integer.parseInt(numberOfHwNodesString));
        configuration.setState(new State(new ZooKeeperStateInterfaceImpl(mesosStateZkUrl)));
        configuration.setMem(Double.parseDouble(ram));
        configuration.setManagementApiPort(Integer.parseInt(managementApiPort));
    }

    private void printUsageAndExit() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(configuration.getFrameworkName(), options);
        System.exit(2);
    }

}
