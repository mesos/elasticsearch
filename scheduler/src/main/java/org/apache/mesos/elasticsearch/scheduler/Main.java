package org.apache.mesos.elasticsearch.scheduler;

import org.apache.commons.cli.*;
import org.apache.mesos.elasticsearch.common.zookeeper.exception.ZKAddressException;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.MesosStateZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.ZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;

/**
 * Application which starts the Elasticsearch scheduler
 */
public class Main {

    public static final String NUMBER_OF_HARDWARE_NODES = "n";

    public static final String ZK_ADDRESS = "zk";

    private Options options;

    private Configuration configuration;

    public Main() {
        this.options = new Options();
        this.options.addOption(NUMBER_OF_HARDWARE_NODES, "numHardwareNodes", true, "number of hardware nodes");
        this.options.addOption(ZK_ADDRESS, "ZookeeperNode", true, "Zookeeper address (IP:PORT/mesos)");
    }

    public static void main(String[] args) {
        Main main = new Main();
        main.run(args);
    }

    public void run(String[] args) {
        try {
            parseCommandlineOptions(args);
        } catch (ParseException | IllegalArgumentException e) {
            printUsage();
            return;
        }

        final ElasticsearchScheduler scheduler = new ElasticsearchScheduler(configuration, new TaskInfoFactory());
        scheduler.run();
    }

    private void parseCommandlineOptions(String[] args) throws ParseException, IllegalArgumentException {
        configuration = new Configuration();

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        String numberOfHwNodesString = cmd.getOptionValue(NUMBER_OF_HARDWARE_NODES);
        String zkAddress = cmd.getOptionValue(ZK_ADDRESS);

        if (numberOfHwNodesString == null || zkAddress == null) {
            printUsage();
            return;
        }
        String formattedAddr;
        try {
            ZKFormatter formatter = new MesosStateZKFormatter(new ZKAddressParser());
            formattedAddr = formatter.format(zkAddress);
        } catch (ZKAddressException ex) {
            throw new ParseException("Incorrect ZK address format: " + ex.getMessage());
        }
        System.out.println("ZK ADDRESSES: " + zkAddress + " and " + formattedAddr);
        configuration.setZookeeperAddress(formattedAddr);
        System.out.println("So config says: " + configuration.getZookeeperAddress());
        configuration.setVersion(getClass().getPackage().getImplementationVersion());
        configuration.setNumberOfHwNodes(Integer.parseInt(numberOfHwNodesString));
        configuration.setState(new State(new ZooKeeperStateInterfaceImpl(configuration.getZookeeperAddress())));
    }

    private void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(configuration.getFrameworkName(), options);
    }

}
