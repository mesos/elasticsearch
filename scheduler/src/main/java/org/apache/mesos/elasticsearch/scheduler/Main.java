package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.elasticsearch.scheduler.cluster.ClusterMonitor;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.util.HashMap;

/**
 * Application which starts the Elasticsearch scheduler
 */
public class Main {
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

        final FrameworkState frameworkState = new FrameworkState(configuration.getZooKeeperStateDriver());
        final ClusterState clusterState = new ClusterState(configuration.getZooKeeperStateDriver(), frameworkState);

        final ElasticsearchScheduler scheduler = new ElasticsearchScheduler(configuration, frameworkState, new TaskInfoFactory());
        final ClusterMonitor clusterMonitor = new ClusterMonitor(configuration, frameworkState, scheduler);

        scheduler.addObserver(clusterState);
        scheduler.addObserver(clusterMonitor);

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
