package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.util.ProtoTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.NotSerializableException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * `Tests
 */
@SuppressWarnings({"PMD.TooManyMethods"})
public class ClusterStateTest {
    private SerializableState state = mock(SerializableState.class);
    private FrameworkState frameworkState = mock(FrameworkState.class);
    private ClusterState clusterState = new ClusterState(state, frameworkState);

    @Before
    public void before() throws NotSerializableException {
        Protos.FrameworkID frameworkID = Protos.FrameworkID.newBuilder().setValue("FrameworkID").build();
        when(frameworkState.getFrameworkID()).thenReturn(frameworkID);
        when(state.get(anyString())).thenReturn(new ArrayList<Protos.TaskInfo>());
    }

    @Test
    public void shouldGetListFromZK() throws NotSerializableException {
        List<Protos.TaskInfo> taskList = clusterState.getTaskList();
        verify(state, times(1)).get(anyString());
        assertEquals(0, taskList.size());
    }

    @Test
    public void shouldHandleGetException() throws NotSerializableException {
        when(state.get(anyString())).thenThrow(NotSerializableException.class);
        List<Protos.TaskInfo> taskList = clusterState.getTaskList();
        verify(state, times(1)).get(anyString());
        assertEquals(0, taskList.size());
    }

    @Test
    public void shouldHandleGetNull() throws NotSerializableException {
        when(state.get(anyString())).thenReturn(null);
        List<Protos.TaskInfo> taskList = clusterState.getTaskList();
        verify(state, times(1)).get(anyString());
        assertEquals(0, taskList.size());
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldThrowExceptionWhenGetStatusTaskIDDesntExist() throws NotSerializableException {
        clusterState.getStatus(Protos.TaskID.newBuilder().setValue("").build());
    }

    @Test
    public void shouldReturnStatusWhenDoesExist() throws NotSerializableException {
        ArrayList<Protos.TaskInfo> taskInfos = new ArrayList<>();
        taskInfos.add(ProtoTestUtil.getDefaultTaskInfo());
        Protos.TaskInfo defaultTaskInfo = ProtoTestUtil.getDefaultTaskInfo();
        taskInfos.add(defaultTaskInfo);
        when(state.get(anyString())).thenReturn(taskInfos);
        ESTaskStatus status = clusterState.getStatus(defaultTaskInfo.getTaskId());
        assertNotNull(status);
    }

    @Test
    public void shouldAddTask() throws NotSerializableException {
        ArrayList<Protos.TaskInfo> mock = Mockito.spy(new ArrayList<>());
        when(state.get(anyString())).thenReturn(mock);
        Protos.TaskInfo defaultTaskInfo = ProtoTestUtil.getDefaultTaskInfo();
        clusterState.addTask(defaultTaskInfo);
        verify(state, times(1)).set(anyString(), any());
        verify(mock, times(1)).add(eq(defaultTaskInfo));
    }

    @Test
    public void shouldHandleExceptionWhenAddingTask() throws NotSerializableException {
        ArrayList<Protos.TaskInfo> mock = Mockito.spy(new ArrayList<>());
        when(state.get(anyString())).thenReturn(mock);
        doThrow(NotSerializableException.class).when(state).set(anyString(), any());
        Protos.TaskInfo defaultTaskInfo = ProtoTestUtil.getDefaultTaskInfo();
        clusterState.addTask(defaultTaskInfo);
    }

    @Test
    public void shouldDeleteTask() throws NotSerializableException {
        ArrayList<Protos.TaskInfo> mock = Mockito.spy(new ArrayList<>());
        Protos.TaskInfo defaultTaskInfo = ProtoTestUtil.getDefaultTaskInfo();
        mock.add(defaultTaskInfo);
        when(state.get(anyString())).thenReturn(mock);
        clusterState.removeTask(defaultTaskInfo);
        verify(state, times(1)).set(anyString(), any());
        verify(mock, times(1)).remove(eq(defaultTaskInfo));
    }

    @Test
    public void shouldReturnTrueIfExists() throws NotSerializableException {
        ArrayList<Protos.TaskInfo> mock = Mockito.spy(new ArrayList<>());
        Protos.TaskInfo defaultTaskInfo = ProtoTestUtil.getDefaultTaskInfo();
        mock.add(defaultTaskInfo);
        when(state.get(anyString())).thenReturn(mock);
        assertTrue(clusterState.exists(defaultTaskInfo.getTaskId()));
        verify(state, times(1)).get(anyString());
    }

    @Test
    public void shouldReturnFalseIfNotExists() throws NotSerializableException {
        Protos.TaskInfo defaultTaskInfo = ProtoTestUtil.getDefaultTaskInfo();
        assertFalse(clusterState.exists(defaultTaskInfo.getTaskId()));
        verify(state, times(1)).get(anyString());
    }
}