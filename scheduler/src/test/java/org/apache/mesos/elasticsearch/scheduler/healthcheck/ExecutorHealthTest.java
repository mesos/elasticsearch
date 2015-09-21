package org.apache.mesos.elasticsearch.scheduler.healthcheck;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.scheduler.state.ESTaskStatus;
import org.apache.mesos.elasticsearch.scheduler.util.ProtoTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Tests executor health
 */
public class ExecutorHealthTest {
    @Mock
    private Scheduler scheduler;
    @Mock
    private SchedulerDriver schedulerDriver;
    @Mock
    private ESTaskStatus taskStatus;
    private ExecutorHealth executorHealth;

    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
        executorHealth = new ExecutorHealth(scheduler, schedulerDriver, taskStatus, 5000L);
        when(taskStatus.getStatus()).thenReturn(initialTaskStatus());
    }

    @Test
    public void timeoutShouldNotResetWhenChecking() {
        Long lastUpdate = runAndGetLastUpdate(executorHealth);
        when(taskStatus.getStatus()).thenReturn(overdueTaskStatus());
        Long thisUpdate = runAndGetLastUpdate(executorHealth);
        assertEquals(lastUpdate, thisUpdate);
    }

    @Test
    public void shouldCallExecutorLostWhenTimeout() {
        runAndGetLastUpdate(executorHealth);
        when(taskStatus.getStatus()).thenReturn(overdueTaskStatus());
        when(taskStatus.getTaskInfo()).thenReturn(ProtoTestUtil.getDefaultTaskInfo());
        runAndGetLastUpdate(executorHealth);
        verify(scheduler, times(1)).executorLost(eq(schedulerDriver), any(), any(), anyInt());
    }

    @Test
    public void shouldNotCallExecutorLostWhenNotTimedOut() {
        Long lastUpdate = runAndGetLastUpdate(executorHealth);
        when(taskStatus.getStatus()).thenReturn(underDueTaskStatus());
        Long thisUpdate = runAndGetLastUpdate(executorHealth);
        assertTrue("" + thisUpdate + " should be greater than " + lastUpdate, thisUpdate > lastUpdate);
        verify(scheduler, times(0)).executorLost(eq(schedulerDriver), any(), any(), anyInt());
    }

    // If the time was zero, that means the value was not set.
    @Test
    public void shouldNotUpdateIfTimeWasZero() {
        Long initialLastUpdate = executorHealth.getLastUpdate();
        when(taskStatus.getStatus()).thenReturn(taskStatus(0.0));
        Long lastUpdate = runAndGetLastUpdate(executorHealth);
        assertEquals(initialLastUpdate, lastUpdate);
    }

    private Protos.TaskStatus overdueTaskStatus() {
        return taskStatus(10.0);
    }

    private Protos.TaskStatus underDueTaskStatus() {
        return taskStatus(2.0);
    }

    private Protos.TaskStatus initialTaskStatus() {
        return taskStatus(1.0);
    }

    private Protos.TaskStatus taskStatus(Double timestamp) {
        return ProtoTestUtil.getDefaultTaskStatus(Protos.TaskState.TASK_RUNNING, timestamp);
    }

    private Long runAndGetLastUpdate(ExecutorHealth executorHealth) {
        executorHealth.run();
        return executorHealth.getLastUpdate();
    }
}