package org.apache.mesos.elasticsearch.scheduler;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Application which starts the Elasticsearch scheduler
 */
public class Main {

    public static final String NUMBER_OF_HARDWARE_NODES = "n";

    public static final String ZK_HOST = "zk";

    private Options options;

    private Configuration configuration;

    public Main() {
        this.options = new Options();
        this.options.addOption(NUMBER_OF_HARDWARE_NODES, "numHardwareNodes", true, "number of hardware nodes");
        this.options.addOption(ZK_HOST, "ZookeeperNode", true, "Zookeeper IP address and port");
    }

    public static void main(String[] args) {
        Main main = new Main();
        main.run(args);
    }

    public void run(String[] args) {
        parseCommandlineOptions(args);

        final ElasticsearchScheduler scheduler = new ElasticsearchScheduler(configuration, new TaskInfoFactory());
        scheduler.run();
    }

    private void parseCommandlineOptions(String[] args) {
        configuration = new Configuration();

        try {
            CommandLineParser parser = new BasicParser();
            CommandLine cmd = parser.parse(options, args);

            String numberOfHwNodesString = cmd.getOptionValue(NUMBER_OF_HARDWARE_NODES);
            String zkHost = cmd.getOptionValue(ZK_HOST);

            if (numberOfHwNodesString == null || zkHost == null) {
                printUsage();
                return;
            }

            configuration.setVersion(getClass().getPackage().getImplementationVersion());
            configuration.setNumberOfHwNodes(Integer.parseInt(numberOfHwNodesString));
            configuration.setZookeeperHost(zkHost);
            configuration.setState(new State(new ZooKeeperStateInterfaceImpl(zkHost + ":" + configuration.getZookeeperPort())));
        } catch (ParseException | IllegalArgumentException e) {
            printUsage();
        }
    }

    private void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(configuration.getFrameworkName(), options);
    }

}
