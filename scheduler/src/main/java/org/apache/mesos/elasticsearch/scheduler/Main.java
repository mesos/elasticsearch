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

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("n", "numHardwareNodes", true, "number of hardware nodes");
        options.addOption("zk", "ZookeeperNode", true, "Zookeeper IP address and port");

        org.apache.mesos.elasticsearch.scheduler.Configuration configuration = new org.apache.mesos.elasticsearch.scheduler.Configuration();

        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            String numberOfHwNodesString = cmd.getOptionValue("n");
            String zkHost = cmd.getOptionValue("zk");
            if (numberOfHwNodesString == null || zkHost == null) {
                printUsage(configuration, options);
                return;
            }

            try {
                configuration.setNumberOfHwNodes(Integer.parseInt(numberOfHwNodesString));
            } catch (IllegalArgumentException e) {
                printUsage(configuration, options);
                return;
            }

            configuration.setZookeeperHost(zkHost);
            configuration.setState(new State(new ZooKeeperStateInterfaceImpl(zkHost + ":" + configuration.getZookeeperPort())));

            final ElasticsearchScheduler scheduler = new ElasticsearchScheduler(configuration, new TaskInfoFactory());

            Runtime.getRuntime().addShutdownHook(new Thread(scheduler::onShutdown));
            Thread schedThred = new Thread(scheduler);
            schedThred.start();
            scheduler.waitUntilInit();
        } catch (ParseException e) {
            printUsage(configuration, options);
        }
    }

    private static void printUsage(org.apache.mesos.elasticsearch.scheduler.Configuration configuration, Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(configuration.getFrameworkName(), options);
    }

}
