package org.apache.mesos.elasticsearch.scheduler;

import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.cluster.ClusterMonitor;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.apache.mesos.elasticsearch.scheduler.state.SerializableZookeeperState;
import org.apache.mesos.state.ZooKeeperState;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * Application which starts the Elasticsearch scheduler
 */
public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class);

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
            final SimpleFileServer simpleFileServer = new SimpleFileServer(Configuration.ES_EXECUTOR_JAR);
            simpleFileServer.run();
            configuration.setFrameworkFileServerAddress(simpleFileServer.getAddress());
        }

        final SerializableZookeeperState zookeeperStateDriver = new SerializableZookeeperState(new ZooKeeperState(
                configuration.getMesosStateZKURL(),
                configuration.getZookeeperCLI().getZookeeperMesosTimeout(),
                TimeUnit.MILLISECONDS,
                "/" + configuration.getFrameworkName() + "/" + configuration.getElasticsearchCLI().getElasticsearchClusterName()));
        final FrameworkState frameworkState = new FrameworkState(zookeeperStateDriver);
        final ClusterState clusterState = new ClusterState(zookeeperStateDriver, frameworkState);
        final TaskInfoFactory taskInfoFactory = new TaskInfoFactory(clusterState);

        final ElasticsearchScheduler scheduler = new ElasticsearchScheduler(
                configuration,
                frameworkState,
                clusterState,
                taskInfoFactory,
                configuration.getExternalVolumeDriver() != null && configuration.getExternalVolumeDriver().length() > 0 ? new OfferStrategyExternalStorage(configuration, clusterState) : new OfferStrategyNormal(configuration, clusterState),
                zookeeperStateDriver);
        new ClusterMonitor(configuration, frameworkState, zookeeperStateDriver, scheduler);


        FrameworkInfoFactory frameworkInfoFactory = new FrameworkInfoFactory(configuration, frameworkState);
        final Protos.FrameworkInfo.Builder frameworkBuilder = frameworkInfoFactory.getBuilder();
        final Protos.Credential.Builder credentialBuilder = new CredentialFactory(configuration).getBuilder();
        final MesosSchedulerDriver schedulerDriver;
        if (credentialBuilder.isInitialized()) {
            LOGGER.debug("Creating Scheduler driver with principal: " + credentialBuilder.toString());
            schedulerDriver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), configuration.getMesosZKURL(), credentialBuilder.build());
        } else {
            schedulerDriver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), configuration.getMesosZKURL());
        }

        HashMap<String, Object> properties = new HashMap<>();
        properties.put("server.port", String.valueOf(configuration.getWebUiPort()));
        new SpringApplicationBuilder(WebApplication.class)
                .properties(properties)
                .initializers(applicationContext -> applicationContext.getBeanFactory().registerSingleton("scheduler", scheduler))
                .initializers(applicationContext -> applicationContext.getBeanFactory().registerSingleton("configuration", configuration))
                .initializers(applicationContext -> applicationContext.getBeanFactory().registerSingleton("frameworkState", frameworkState))
                .showBanner(false)
                .run(args);

        scheduler.run(schedulerDriver);
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
