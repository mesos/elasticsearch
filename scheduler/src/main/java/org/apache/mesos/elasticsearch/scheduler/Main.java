package org.apache.mesos.elasticsearch.scheduler;

import com.beust.jcommander.JCommander;
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

    private Configuration configuration;

    public static void main(String[] args) {
        Main main = new Main();
        main.run(args);
    }

    public void run(String[] args) {
        checkEnv();

        configuration = new Configuration();
        final JCommander jCommander = new JCommander(configuration);
        try {
            jCommander.parse(args); // Parse command line args into configuration class.
        } catch (com.beust.jcommander.ParameterException ex) {
            System.out.println(ex);
            jCommander.setProgramName("(Options preceded by an asterisk are required)");
            jCommander.usage();
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

    private void checkEnv() {
        Map<String, String> env = System.getenv();
        checkHeap(env.get(ExecutorEnvironmentalVariables.JAVA_OPTS));
    }

    private void checkHeap(String s) {
        if (s == null || s.isEmpty()) {
            throw new IllegalArgumentException("Scheduler heap space not set!");
        }
    }
}
