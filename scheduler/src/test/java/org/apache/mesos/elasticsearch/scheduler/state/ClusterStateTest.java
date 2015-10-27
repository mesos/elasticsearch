package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.TaskInfoFactory;
import org.apache.mesos.elasticsearch.scheduler.util.ProtoTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
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

    private ClusterState clusterState = new ClusterState(state, frameworkState, mock(TaskInfoFactory.class));

    @Before
    public void before() throws IOException {
        Protos.FrameworkID frameworkID = Protos.FrameworkID.newBuilder().setValue("FrameworkID").build();
        when(frameworkState.getFrameworkID()).thenReturn(frameworkID);
    }

    @Test
    public void shouldGetListFromZK() throws IOException {
        List<Protos.TaskInfo> taskList = clusterState.getTaskList();
        verify(state, times(1)).get(anyString());
        assertEquals(0, taskList.size());
    }

    @Test
    public void shouldHandleGetException() throws IOException {
        when(state.<List<String>>get(anyString())).thenThrow(new IOException("Test exception"));
        List<Protos.TaskInfo> taskList = clusterState.getTaskList();
        verify(state, times(1)).get(anyString());
        assertEquals(0, taskList.size());
    }

    @Test
    public void shouldHandleGetNull() throws IOException {
        when(state.get(anyString())).thenReturn(null);
        List<Protos.TaskInfo> taskList = clusterState.getTaskList();
        verify(state, times(1)).get(anyString());
        assertEquals(0, taskList.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenGetStatusTaskIDDesntExist() throws IOException {
        clusterState.getStatus(Protos.TaskID.newBuilder().setValue("").build());
    }

    @Test
    public void shouldReturnStatusWhenDoesExist() throws IOException {
        ArrayList<Protos.TaskInfo> taskInfos = new ArrayList<>();
        taskInfos.add(ProtoTestUtil.getDefaultTaskInfo());
        Protos.TaskInfo defaultTaskInfo = ProtoTestUtil.getDefaultTaskInfo();
        taskInfos.add(defaultTaskInfo);
        when(state.get(anyString())).thenReturn(taskInfos).thenReturn(ProtoTestUtil.getDefaultTaskStatus(Protos.TaskState.TASK_FINISHED));
        ESTaskStatus status = clusterState.getStatus(defaultTaskInfo.getTaskId());
        assertNotNull(status);
    }

    @Test
    public void shouldAddTask() throws IOException {
        ArrayList<Protos.TaskInfo> mock = Mockito.spy(new ArrayList<>());
        when(state.get(anyString())).thenReturn(mock);
        Protos.TaskInfo defaultTaskInfo = ProtoTestUtil.getDefaultTaskInfo();
        clusterState.addTask(defaultTaskInfo);
        verify(state, times(1)).set(anyString(), any());
        verify(mock, times(1)).add(eq(defaultTaskInfo));
    }

    @Test
    public void shouldHandleExceptionWhenAddingTask() throws IOException {
        ArrayList<Protos.TaskInfo> mock = Mockito.spy(new ArrayList<>());
        when(state.get(anyString())).thenReturn(mock);
        doThrow(IOException.class).when(state).set(anyString(), any());
        Protos.TaskInfo defaultTaskInfo = ProtoTestUtil.getDefaultTaskInfo();
        clusterState.addTask(defaultTaskInfo);
    }

    @Test
    public void shouldDeleteTask() throws IOException {
        ArrayList<Protos.TaskInfo> mock = Mockito.spy(new ArrayList<>());
        Protos.TaskInfo defaultTaskInfo = ProtoTestUtil.getDefaultTaskInfo();
        mock.add(defaultTaskInfo);
        when(state.get(anyString())).thenReturn(mock).thenReturn(ProtoTestUtil.getDefaultTaskStatus(Protos.TaskState.TASK_FINISHED));
        clusterState.removeTask(defaultTaskInfo);
        verify(state, times(1)).set(anyString(), any());
        verify(mock, times(1)).remove(eq(defaultTaskInfo));
    }

    @Test
    public void shouldReturnTrueIfExists() throws IOException {
        ArrayList<Protos.TaskInfo> mock = Mockito.spy(new ArrayList<>());
        Protos.TaskInfo defaultTaskInfo = ProtoTestUtil.getDefaultTaskInfo();
        mock.add(defaultTaskInfo);
        when(state.get(anyString())).thenReturn(mock);
        assertTrue(clusterState.exists(defaultTaskInfo.getTaskId()));
        verify(state, atLeastOnce()).get(anyString());
    }

    @Test
    public void shouldReturnFalseIfNotExists() throws IOException {
        Protos.TaskInfo defaultTaskInfo = ProtoTestUtil.getDefaultTaskInfo();
        assertFalse(clusterState.exists(defaultTaskInfo.getTaskId()));
        verify(state, times(1)).get(anyString());
    }

    // TODO (pnw) Why does this test take so long?
    @Test
    public void shouldReturnCorrectNumberOfExecutors() throws IOException {
        ArrayList<Protos.TaskInfo> mock = Mockito.spy(new ArrayList<>());
        mock.add(ProtoTestUtil.getDefaultTaskInfo());
        Protos.TaskInfo defaultTaskInfo = Protos.TaskInfo.newBuilder().mergeFrom(ProtoTestUtil.getDefaultTaskInfo()).setTaskId(Protos.TaskID.newBuilder().setValue("Task2")).build();
        mock.add(defaultTaskInfo);
        when(state.get(contains(ESTaskStatus.STATE_KEY))).thenReturn(ProtoTestUtil.getDefaultTaskStatus(Protos.TaskState.TASK_RUNNING));
        when(state.get(contains(ClusterState.STATE_LIST))).thenReturn(mock); // Be careful, the state list and state key both have the word state in them. Order is important.
        assertEquals(2, clusterState.getGuiTaskList().size());
        clusterState.removeTask(defaultTaskInfo);
        assertEquals(1, clusterState.getGuiTaskList().size());
    }
}