package org.apache.mesos.elasticsearch.scheduler.cluster;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;
import org.apache.mesos.elasticsearch.scheduler.util.ProtoTestUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * Tests for task reaper
 */
public class TaskReaperTest {

    private SchedulerDriver driver;
    private Configuration config;
    private ClusterState state;

    @Before
    public void before() {
        // Setup mocks
        driver = mock(SchedulerDriver.class);
        when(driver.killTask(any())).thenReturn(Protos.Status.DRIVER_RUNNING);

        config = mock(Configuration.class);
        when(config.getElasticsearchNodes()).thenReturn(2);

        state = mock(ClusterState.class);
        List<Protos.TaskInfo> list = Arrays.asList(ProtoTestUtil.getDefaultTaskInfo(), ProtoTestUtil.getDefaultTaskInfo(), ProtoTestUtil.getDefaultTaskInfo());
        when(state.getTaskList()).thenReturn(list);
    }

    @Test
    public void shouldRemoveTask() {
        // Start task reaper.
        TaskReaper taskReaper = new TaskReaper(driver, config, state);
        taskReaper.run();

        // Should try to kill a task
        verify(driver, atLeastOnce()).killTask(any());
    }

    @Test
    public void shouldNotRemoveTaskWhenEqual() {
        when(config.getElasticsearchNodes()).thenReturn(3); // Override before

        // Start task reaper.
        TaskReaper taskReaper = new TaskReaper(driver, config, state);
        taskReaper.run();

        // Should not call kill
        verify(driver, never()).killTask(any());
    }

    @Test
    public void shouldNotRemoveTaskWhenGreater() {
        when(config.getElasticsearchNodes()).thenReturn(4); // Override before

        // Start task reaper.
        TaskReaper taskReaper = new TaskReaper(driver, config, state);
        taskReaper.run();

        // Should not call kill
        verify(driver, never()).killTask(any());
    }

    @Test
    public void shouldNotKillMoreThanAvailable() {
        when(config.getElasticsearchNodes()).thenReturn(-1); // Override before

        // Start task reaper.
        TaskReaper taskReaper = new TaskReaper(driver, config, state);
        taskReaper.run();

        // Should not call kill
        verify(driver, times(3)).killTask(any());
    }
}