package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.jayway.awaitility.Awaitility;
import org.apache.commons.io.IOUtils;
import org.apache.mesos.elasticsearch.systemtest.base.SchedulerTestBase;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * System test for the executor
 */
public class ExecutorSystemTest extends SchedulerTestBase {

    private DockerUtil dockerUtil = new DockerUtil(CLUSTER.getConfig().dockerClient);

    @Before
    public void before() {
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> dockerUtil.getExecutorContainers().size() > 0); // Make sure executors are alive before performing tests
    }

    /**
     * Make sure that lib mesos exists in /usr/lib/libmesos.so
     * @throws IOException
     */
    @Test
    public void ensureLibMesosExists() throws IOException {
        // Remote execute an ls command to make sure the file exists
        ExecCreateCmdResponse exec = CLUSTER.getConfig().dockerClient.execCreateCmd(getRandomExecutorId()).withAttachStdout().withAttachStderr().withCmd("ls", "/usr/lib/libmesos.so").exec();
        InputStream execCmdStream = CLUSTER.getConfig().dockerClient.execStartCmd(exec.getId()).exec();
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
        ExecCreateCmdResponse exec = CLUSTER.getConfig().dockerClient.execCreateCmd(getRandomExecutorId()).withTty(true).withAttachStdout().withAttachStderr().withCmd("env").exec();
        InputStream execCmdStream = CLUSTER.getConfig().dockerClient.execStartCmd(exec.getId()).exec();
        String result = IOUtils.toString(execCmdStream, "UTF-8");

        // Get MESOS_NATIVE_JAVA_LIBRARY from env
        List<String> env = Arrays.asList(result.split("\n")).stream().filter(s -> s.contains("MESOS_NATIVE_JAVA_LIBRARY")).collect(Collectors.toList());
        assertEquals("env does not have MESOS_NATIVE_JAVA_LIBRARY: " + result, 1, env.size());

        // Remote execute the ENV var to make sure it points to a real file
        String path = env.get(0).split("=")[1].replace("\r", "").replace("\n", "");
        exec = CLUSTER.getConfig().dockerClient.execCreateCmd(getRandomExecutorId()).withTty(true).withAttachStdout().withAttachStderr().withCmd("ls", path).exec();
        execCmdStream = CLUSTER.getConfig().dockerClient.execStartCmd(exec.getId()).exec();
        result = IOUtils.toString(execCmdStream, "UTF-8");
        assertFalse(path + " does not exist: " + result, result.contains("No such file"));
    }

    private String getRandomExecutorId() {
        return dockerUtil.getExecutorContainers().get(0).getId();
    }
}
