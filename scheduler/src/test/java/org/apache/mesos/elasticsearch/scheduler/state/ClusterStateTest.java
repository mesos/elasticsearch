package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.mesos.elasticsearch.scheduler.cluster.ESTask;
import org.apache.mesos.elasticsearch.scheduler.util.ProtoTestUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * `Tests
 */
@SuppressWarnings({"PMD.TooManyMethods"})
public class ClusterStateTest {
    private ESState esState = mock(ESState.class);

    private ClusterState clusterState = new ClusterState(esState);

    @Test
    public void shouldGetListFromZK() throws IOException {
        final List<ESTask> taskList = clusterState.get();
        verify(esState, times(1)).get();
        assertEquals(0, taskList.size());
    }

    @Test
    public void shouldHandleGetNull() throws IOException {
        when(esState.get()).thenReturn(null);
        final List<ESTask> taskList = clusterState.get();
        verify(esState, times(1)).get();
        assertEquals(0, taskList.size());
    }

    @Test
    public void shouldAddTask() throws IOException {
        final ESTask esTask = mock(ESTask.class);
        clusterState.add(esTask);
        verify(esState, times(1)).set(any());
    }

    @Test
    public void shouldDeleteTask() throws IOException {
        final ESTask esTask = mock(ESTask.class);
        clusterState.remove(esTask);
        verify(esTask, times(1)).destroy();
    }

    @Test
    public void shouldIncrementEsNodeId() throws IOException {
        assertEquals(0, clusterState.getElasticNodeId().intValue());
        final ESTask esTask = mock(ESTask.class);
        when(esTask.getTask()).thenReturn(ProtoTestUtil.getTaskInfoExternalVolume(0));
        when(esState.get()).thenReturn(Collections.singletonList(esTask));
        assertEquals(1, clusterState.getElasticNodeId().intValue());
    }

    @Test
    public void shouldReplaceNodeId() throws IOException {
        final ESTask esTask = mock(ESTask.class);
        when(esTask.getTask()).thenReturn(ProtoTestUtil.getTaskInfoExternalVolume(1));
        when(esState.get()).thenReturn(Collections.singletonList(esTask));
        assertEquals(0, clusterState.getElasticNodeId().intValue());
        final ESTask esTask2 = mock(ESTask.class);
        when(esTask2.getTask()).thenReturn(ProtoTestUtil.getTaskInfoExternalVolume(0));
        when(esState.get()).thenReturn(Arrays.asList(esTask2, esTask));
        assertEquals(2, clusterState.getElasticNodeId().intValue());
    }
}