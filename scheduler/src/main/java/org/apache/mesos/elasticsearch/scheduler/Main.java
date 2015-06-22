package org.apache.mesos.elasticsearch.scheduler;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.mesos.elasticsearch.common.Configuration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import java.lang.ref.WeakReference;

/**
 * Application which starts the Elasticsearch scheduler
 */
@EnableAutoConfiguration
@ComponentScan
public class Main {
    private static WeakReference<ElasticsearchScheduler> elasticsearchScheduler;

    @Bean
    public ElasticsearchScheduler getElasticsearchScheduler() {
        return elasticsearchScheduler.get();
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("n", "numHardwareNodes", true, "number of hardware nodes");
        options.addOption("zk", "ZookeeperNode", true, "Zookeeper IP address and port");
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            String numberOfHwNodesString = cmd.getOptionValue("n");
            String zkHost = cmd.getOptionValue("zk");
            if (numberOfHwNodesString == null || zkHost == null) {
                printUsage(options);
                return;
            }
            int numberOfHwNodes;
            try {
                numberOfHwNodes = Integer.parseInt(numberOfHwNodesString);
            } catch (IllegalArgumentException e) {
                printUsage(options);
                return;
            }

            ZooKeeperStateInterface zkState = new ZooKeeperStateInterfaceImpl(zkHost + ":" + Configuration.ZOOKEEPER_PORT);
            State state = new State(zkState);

            final ElasticsearchScheduler scheduler = new ElasticsearchScheduler(numberOfHwNodes, state, zkHost, new TaskInfoFactory());

            elasticsearchScheduler = new WeakReference<>(scheduler);
            final ConfigurableApplicationContext springApplication = SpringApplication.run(Main.class, args);

            Runtime.getRuntime().addShutdownHook(new Thread(scheduler::onShutdown));
            Runtime.getRuntime().addShutdownHook(new Thread(springApplication::close));
            Thread schedThred = new Thread(scheduler);
            schedThred.start();
            scheduler.waitUntilInit();

        } catch (ParseException e) {
            printUsage(options);
        }
    }

    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(Configuration.FRAMEWORK_NAME, options);
    }

}
