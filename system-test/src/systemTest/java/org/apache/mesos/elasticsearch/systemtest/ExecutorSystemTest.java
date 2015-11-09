package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.command.ExecCreateCmd;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.base.SchedulerTestBase;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * System test for the executor
 */
public class ExecutorSystemTest extends SchedulerTestBase {
    private static final Logger LOGGER = Logger.getLogger(ExecutorSystemTest.class);

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
        // Make sure MESOS_NATIVE_JAVA_LIBRARY is in the env vars
        String allEnvVars = getAllEnvVars();
        assertTrue("Env does not contain MESOS_NATIVE_JAVA_LIBRARY", allEnvVars.contains("MESOS_NATIVE_JAVA_LIBRARY"));

        // Remote execute the ENV var to make sure it points to a real file
        String result = getResultOfLs("$MESOS_NATIVE_JAVA_LIBRARY");
        assertFalse("The file located by MESOS_NATIVE_JAVA_LIBRARY does not exist: " + result, result.contains("No such file"));
    }

    private String getResultOfLs(String path) throws IOException {
        ExecCreateCmdResponse exec = getExecForRandomExecutor().withCmd("sh", "-c", "ls " + path).exec();
        return getResultFromExec(exec);
    }

    private ExecCreateCmd getExecForRandomExecutor() {
        return CLUSTER.getConfig().dockerClient.execCreateCmd(getRandomExecutorId()).withTty(true).withAttachStdout().withAttachStderr();
    }

    private List<String> getEnvVars() throws IOException {
        String result = getAllEnvVars();

        // Get MESOS_NATIVE_JAVA_LIBRARY from env
        return Arrays.asList(result.split("\n")).stream().filter(s -> s.contains("MESOS_NATIVE_JAVA_LIBRARY")).collect(Collectors.toList());
    }

    private String getAllEnvVars() throws IOException {
        final ExecCreateCmdResponse exec = getExecForRandomExecutor().withCmd("env").exec();
        return getResultFromExec(exec);
    }

    private String getResultFromExec(ExecCreateCmdResponse exec) throws IOException {
        InputStream execCmdStream = CLUSTER.getConfig().dockerClient.execStartCmd(exec.getId()).exec();
        return IOUtils.toString(execCmdStream, "UTF-8");
    }

    private String getRandomExecutorId() {
        return dockerUtil.getExecutorContainers().get(0).getId();
    }
}
