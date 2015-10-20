package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Link;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import com.containersol.minimesos.container.AbstractContainer;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * System test for the executor
 */
public class ExecutorSystemTest extends TestBase {
    public static final Logger LOGGER = Logger.getLogger(ExecutorSystemTest.class);

    public static final int DOCKER_PORT = 2376;

    private static DockerClient clusterClient;

    private static String executorId;

    @BeforeClass
    public static void beforeClass() {
        String innerDockerHost;

        LOGGER.debug("Local docker environment");
        innerDockerHost = CLUSTER.getMesosMasterContainer().getIpAddress() + ":" + DOCKER_PORT;

        DockerClientConfig.DockerClientConfigBuilder dockerConfigBuilder = DockerClientConfig.createDefaultConfigBuilder().withUri("http://" + innerDockerHost);
        clusterClient = DockerClientBuilder.getInstance(dockerConfigBuilder.build()).build();
        await().atMost(60, TimeUnit.SECONDS).until(() -> {
            try {
                return clusterClient.listContainersCmd().exec().size() > 0;
            } catch (javax.ws.rs.ProcessingException ignored) {
                return false;
            }
        });
        List<Container> containers = clusterClient.listContainersCmd().exec();

        // Find a single executor container
        executorId = "";
        for (Container container : containers) {
            if (container.getImage().contains("executor")) {
                executorId = container.getId();
                break;
            }
        }
    }

    /**
     * Make sure that lib mesos exists in /usr/lib/libmesos.so
     * @throws IOException
     */
    @Test
    public void ensureLibMesosExists() throws IOException {
        // Remote execute an ls command to make sure the file exists
        ExecCreateCmdResponse exec = clusterClient.execCreateCmd(executorId).withAttachStdout().withAttachStderr().withCmd("ls", "/usr/lib/libmesos.so").exec();
        InputStream execCmdStream = clusterClient.execStartCmd(exec.getId()).exec();
        String result = IOUtils.toString(execCmdStream, "UTF-8");
        assertFalse(result.contains("No such file"));
    }

    /**
     * Tests that the scheduler has force set the MESOS_NATIVE_JAVA_LIBRARY and that it points to a file that exists.
     * @throws IOException
     */
    @Test
    public void ensureEnvVarPointsToLibMesos() throws IOException {
        // Get remote env vars
        ExecCreateCmdResponse exec = clusterClient.execCreateCmd(executorId).withAttachStdout().withAttachStderr().withCmd("env").exec();
        InputStream execCmdStream = clusterClient.execStartCmd(exec.getId()).exec();
        String result = IOUtils.toString(execCmdStream, "UTF-8");

        // Get MESOS_NATIVE_JAVA_LIBRARY from env
        List<String> env = Arrays.asList(result.split("\n")).stream().filter(s -> s.contains("MESOS_NATIVE_JAVA_LIBRARY")).collect(Collectors.toList());
        assertEquals(1, env.size());

        // Remote execute the ENV var to make sure it points to a real file
        String path = env.get(0).split("=")[1];
        exec = clusterClient.execCreateCmd(executorId).withAttachStdout().withAttachStderr().withCmd("ls", path).exec();
        execCmdStream = clusterClient.execStartCmd(exec.getId()).exec();
        result = IOUtils.toString(execCmdStream, "UTF-8");
        assertFalse(result.contains("No such file"));

    }
}
