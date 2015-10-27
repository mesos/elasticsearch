package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.elasticsearch.scheduler.cluster.ClusterMonitor;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.apache.mesos.elasticsearch.scheduler.state.SerializableZookeeperState;
import org.apache.mesos.state.ZooKeeperState;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

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

        if (!configuration.isFrameworkUseDocker()) {
            try {
                final SimpleFileServer simpleFileServer = new SimpleFileServer();
                simpleFileServer.run();
                configuration.setFrameworkFileServerAddress(simpleFileServer.getAddress());
            } catch (UnknownHostException e) {
                throw new IllegalStateException("Unable to start file server. See stack trace.", e);
            }
        }

        final SerializableZookeeperState zookeeperStateDriver = new SerializableZookeeperState(new ZooKeeperState(
                configuration.getMesosStateZKURL(),
                configuration.getZookeeperCLI().getZookeeperMesosTimeout(),
                TimeUnit.MILLISECONDS,
                "/" + configuration.getFrameworkName() + "/" + configuration.getElasticsearchCLI().getElasticsearchClusterName()));
        final TaskInfoFactory taskInfoFactory = new TaskInfoFactory();
        final FrameworkState frameworkState = new FrameworkState(zookeeperStateDriver, taskInfoFactory);
        final ClusterState clusterState = new ClusterState(zookeeperStateDriver, frameworkState, taskInfoFactory);

        final ElasticsearchScheduler scheduler = new ElasticsearchScheduler(
                configuration,
                frameworkState,
                clusterState,
                taskInfoFactory,
                new OfferStrategy(configuration, clusterState),
                zookeeperStateDriver
        );
        new ClusterMonitor(configuration, frameworkState, zookeeperStateDriver, scheduler);

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
