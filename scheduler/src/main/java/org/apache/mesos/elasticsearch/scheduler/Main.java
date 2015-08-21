package org.apache.mesos.elasticsearch.scheduler;

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
    private final Environment env;

    private Configuration configuration;

    public Main(Environment env) {
        this.env = env;
    }

    public static void main(String[] args) {
        Main main = new Main(new Environment());
        main.run(args);
    }

    public void run(String[] args) {
        checkEnv();

        configuration = new Configuration(args);

        final ElasticsearchScheduler scheduler = new ElasticsearchScheduler(configuration, new TaskInfoFactory());

        HashMap<String, Object> properties = new HashMap<>();
        properties.put("server.port", String.valueOf(configuration.getWebUiPort()));
        new SpringApplicationBuilder(WebApplication.class)
                .properties(properties)
                .initializers(applicationContext -> applicationContext.getBeanFactory().registerSingleton("scheduler", scheduler))
                .initializers(applicationContext -> applicationContext.getBeanFactory().registerSingleton("configuration", configuration))
                .showBanner(false)
                .run(args);

        scheduler.run();
    }

    private void checkEnv() {
        checkHeap(env.getJavaHeap());
    }

    private void checkHeap(String s) {
        if (s == null || s.isEmpty()) {
            throw new IllegalArgumentException("Scheduler heap space not set!");
        }
    }
}
