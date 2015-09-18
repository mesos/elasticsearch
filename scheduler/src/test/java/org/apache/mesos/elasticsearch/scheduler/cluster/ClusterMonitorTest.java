package org.apache.mesos.elasticsearch.scheduler.cluster;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.state.StatePath;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.eq;
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

    public static final String FRAMEWORK_ID = "frameworkId";
    public static final String EXECUTOR_ID = "executorId";
    public static final String TASK_ID = "task1";
    public static final String SLAVE_ID = "slaveID";
    private ClusterMonitor clusterMonitor;

    @Before
    public void before() throws IOException {
        MockitoAnnotations.initMocks(this);
        when(configuration.getFrameworkId()).thenReturn(frameworkId());
        when(configuration.getState().get(anyString())).thenReturn(taskStatus(Protos.TaskState.TASK_RUNNING));
        when(configuration.getExecutorHealthDelay()).thenReturn(10L);
        when(configuration.getExecutorTimeout()).thenReturn(20L);
        when(clusterState.getTask(taskInfo().getTaskId())).thenReturn(taskInfo());

        clusterMonitor = new ClusterMonitor(configuration, scheduler, schedulerDriver, statePath);
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
        clusterMonitor.update(null, taskStatus(Protos.TaskState.TASK_FAILED));
        assertEquals(0, clusterMonitor.getHealthChecks().size());
    }

    @Test
    public void shouldCatchIfTryingToRemoveTaskThatIsntMonitored() {
        when(clusterState.getTask(taskInfo().getTaskId())).thenThrow(IllegalArgumentException.class);
        clusterMonitor.update(null, taskStatus(Protos.TaskState.TASK_FAILED));
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
        return Protos.TaskStatus.newBuilder().setSlaveId(slaveID).setTaskId(taskID).setExecutorId(executorID)
                .setState(state).build();
    }

}