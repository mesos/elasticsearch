package org.apache.mesos.elasticsearch.scheduler.cluster;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.apache.mesos.elasticsearch.scheduler.state.SerializableState;
import org.apache.mesos.elasticsearch.scheduler.state.StatePath;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Cluster monitor tests
 */
@SuppressWarnings("PMD.TooManyMethods")
public class ClusterMonitorTest {
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Configuration configuration;
    @Mock
    private Scheduler scheduler;
    @Mock
    private SchedulerDriver schedulerDriver;
    @Mock
    private ClusterState clusterState;
    @Mock
    private StatePath statePath;
    @Mock
    private FrameworkState frameworkState;
    @Mock
    private SerializableState serializableState;

    public static final String FRAMEWORK_ID = "frameworkId";
    public static final String EXECUTOR_ID = "executorId";
    public static final String TASK_ID = "task1";
    public static final String SLAVE_ID = "slaveID";
    private ClusterMonitor clusterMonitor;
    private Consumer<Protos.TaskStatus> onStatusUpdateConsumer;

    @Before
    public void before() throws IOException {
        MockitoAnnotations.initMocks(this);

        when(frameworkState.getFrameworkID()).thenReturn(frameworkId());
        when(frameworkState.getDriver()).thenReturn(schedulerDriver);
        when(configuration.getExecutorHealthDelay()).thenReturn(10L);
        when(configuration.getExecutorTimeout()).thenReturn(20L);

        when(serializableState.get(anyString())).thenReturn(taskStatus(Protos.TaskState.TASK_RUNNING));
        when(clusterState.getTask(taskInfo().getTaskId())).thenReturn(taskInfo());

        clusterMonitor = new ClusterMonitor(configuration, frameworkState, serializableState, scheduler);

        final ArgumentCaptor<Consumer> onStatusUpdateCapture = ArgumentCaptor.forClass(Consumer.class);
        verify(frameworkState).onStatusUpdate(onStatusUpdateCapture.capture());
        onStatusUpdateConsumer = onStatusUpdateCapture.getValue();
    }

    @After
    public void shutdownHealthChecks() {
        clusterMonitor.getHealthChecks().forEach((taskInfo, asyncPing) -> asyncPing.stop());
    }

    @Test
    public void shouldAddTaskToList() {
        Protos.TaskInfo taskInfo = taskInfo();
        clusterMonitor.startMonitoringTask(taskInfo);
        assertEquals(1, clusterMonitor.getHealthChecks().size());
    }

    @Test
    public void shouldRemoveTaskFromList() {
        Protos.TaskInfo taskInfo = taskInfo();
        clusterMonitor.startMonitoringTask(taskInfo);
        assertEquals(1, clusterMonitor.getHealthChecks().size());

        when(clusterState.exists(eq(taskInfo.getTaskId()))).thenReturn(true);
        when(clusterState.taskInError(any())).thenReturn(true);

        onStatusUpdateConsumer.accept(taskStatus(Protos.TaskState.TASK_FAILED));
        assertEquals(0, clusterMonitor.getHealthChecks().size());
    }

    @Test
    public void shouldCatchIfTryingToRemoveTaskThatIsntMonitored() {
        when(clusterState.getTask(taskInfo().getTaskId())).thenThrow(IllegalArgumentException.class);
        onStatusUpdateConsumer.accept(taskStatus(Protos.TaskState.TASK_FAILED));
    }

    private Protos.TaskInfo taskInfo() {
        frameworkId();
        Protos.SlaveID slaveID = Protos.SlaveID.newBuilder().setValue(SLAVE_ID).build();
        Protos.ExecutorID executorID = Protos.ExecutorID.newBuilder().setValue(EXECUTOR_ID).build();
        Protos.TaskID taskID = Protos.TaskID.newBuilder().setValue(TASK_ID).build();
        return Protos.TaskInfo.newBuilder()
                .setTaskId(taskID)
                .setExecutor(Protos.ExecutorInfo.newBuilder()
                                .setExecutorId(executorID)
                                .setCommand(Protos.CommandInfo.getDefaultInstance())
                )
                .setSlaveId(slaveID)
                .setName("Test")
                .build();
    }

    private Protos.FrameworkID frameworkId() {
        return Protos.FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build();
    }

    private Protos.TaskStatus taskStatus(Protos.TaskState state) {
        Protos.SlaveID slaveID = Protos.SlaveID.newBuilder().setValue(SLAVE_ID).build();
        Protos.ExecutorID executorID = Protos.ExecutorID.newBuilder().setValue(EXECUTOR_ID).build();
        Protos.TaskID taskID = Protos.TaskID.newBuilder().setValue(TASK_ID).build();
        return Protos.TaskStatus.newBuilder()
                .setSlaveId(slaveID)
                .setTaskId(taskID)
                .setExecutorId(executorID)
                .setTimestamp(1.0)
                .setState(state).build();
    }

}