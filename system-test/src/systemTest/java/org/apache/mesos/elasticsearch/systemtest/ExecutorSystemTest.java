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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * System test for the executor
 */
public class ExecutorSystemTest extends SchedulerTestBase {

    private DockerUtil dockerUtil = new DockerUtil(CLUSTER.getConfig().dockerClient);

    /**
     * Make sure that lib mesos exists in /usr/lib/libmesos.so
     * @throws IOException
     */
    @Test
    public void ensureLibMesosExists() throws IOException {
        // Remote execute an ls command to make sure the file exists
        String path = "/usr/lib/libmesos.so";
        String result = getResultOfLs(path);
        assertFalse(result.contains("No such file"));
    }

    /**
     * Tests that the scheduler has force set the MESOS_NATIVE_JAVA_LIBRARY and that it points to a file that exists.
     * @throws IOException
     */
    @Test
    public void ensureEnvVarPointsToLibMesos() throws IOException {
        // Get remote env vars
        final ExecCreateCmdResponse exec = CLUSTER.getConfig().dockerClient.execCreateCmd(getRandomExecutorId()).withTty(true).withAttachStdout().withAttachStderr().withCmd("env").exec();

        Awaitility.await().atMost(1L, TimeUnit.MINUTES).until(() -> {
                    List<String> env = getEnvVars(exec);
                    return env.size() > 0;
                });

        List<String> env = getEnvVars(exec);
        assertTrue("env does not have MESOS_NATIVE_JAVA_LIBRARY.", env.size() > 0);

        // Remote execute the ENV var to make sure it points to a real file
        String path = env.get(0).split("=")[1].replace("\r", "").replace("\n", "");
        String result = getResultOfLs(path);
        assertFalse(path + " does not exist: " + result, result.contains("No such file"));
    }

    public String getResultOfLs(String path) throws IOException {
        ExecCreateCmdResponse exec2 = CLUSTER.getConfig().dockerClient.execCreateCmd(getRandomExecutorId()).withTty(true).withAttachStdout().withAttachStderr().withCmd("ls", path).exec();
        InputStream execCmdStream = CLUSTER.getConfig().dockerClient.execStartCmd(exec2.getId()).exec();
        return IOUtils.toString(execCmdStream, "UTF-8");
    }

    public List<String> getEnvVars(ExecCreateCmdResponse exec) throws IOException {
        InputStream execCmdStream = CLUSTER.getConfig().dockerClient.execStartCmd(exec.getId()).exec();
        String result = IOUtils.toString(execCmdStream, "UTF-8");

        // Get MESOS_NATIVE_JAVA_LIBRARY from env
        return Arrays.asList(result.split("\n")).stream().filter(s -> s.contains("MESOS_NATIVE_JAVA_LIBRARY")).collect(Collectors.toList());
    }

    private String getRandomExecutorId() {
        return dockerUtil.getExecutorContainers().get(0).getId();
    }
}
