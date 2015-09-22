package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.util.ProtoTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.security.InvalidParameterException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Tests
 */
@SuppressWarnings({"PMD.TooManyMethods"})
public class ESTaskStatusTest {
    private SerializableState state = mock(SerializableState.class);
    private Protos.FrameworkID frameworkID;
    private Protos.TaskInfo taskInfo;
    private ESTaskStatus status;
    private Protos.TaskStatus taskStatus;

    @Before
    public void before() {
        frameworkID = Protos.FrameworkID.newBuilder().setValue("FrameworkId").build();
        taskInfo = ProtoTestUtil.getDefaultTaskInfo();
        status = new ESTaskStatus(state, frameworkID, taskInfo, new StatePath(state));
        taskStatus = status.getDefaultStatus();
    }

    @Test
    public void shouldCheckIfStatusIsValid() throws IOException {
        Mockito.reset(state);
        when(state.get(anyString())).thenThrow(IllegalStateException.class).thenReturn(taskStatus);
        status = new ESTaskStatus(state, frameworkID, taskInfo, new StatePath(state));
        verify(state, atLeastOnce()).set(anyString(), any());
    }

    @Test(expected = IllegalStateException.class)
    public void testHandleSetException() throws IllegalStateException, IOException {
        doThrow(IOException.class).when(state).set(anyString(), any());
        status.setStatus(taskStatus);
    }

    @Test(expected = IllegalStateException.class)
    public void testHandleGetException() throws IllegalStateException, IOException {
        doThrow(IOException.class).when(state).get(anyString());
        status.getStatus();
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptIfStateIsNull() {
        new ESTaskStatus(null, frameworkID, taskInfo, new StatePath(state));
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptIfFrameworkIDIsNull() {
        new ESTaskStatus(state, null, taskInfo, new StatePath(state));
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptIfFrameworkIDIsEmpty() {
        new ESTaskStatus(state, Protos.FrameworkID.newBuilder().setValue("").build(), taskInfo, new StatePath(state));
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptIfTaskInfoIsNull() {
        new ESTaskStatus(state, frameworkID, null, new StatePath(state));
    }

    @Test
    public void shouldPrintOk() throws IOException {
        when(state.get(anyString())).thenReturn(status.getDefaultStatus());
        String s = status.toString();
        Assert.assertTrue(s.contains(status.getDefaultStatus().getState().toString()));
        Assert.assertTrue(s.contains(ESTaskStatus.DEFAULT_STATUS_NO_MESSAGE_SET));
    }

    @Test
    public void shouldHandleNoMessageError() {
        status.toString();
    }

    @Test
    public void shouldErrorWhenTaskFinishedToUpdateState() throws IOException {
        when(state.get(anyString())).thenReturn(ProtoTestUtil.getDefaultTaskStatus(Protos.TaskState.TASK_FINISHED));
        Assert.assertTrue(status.taskInError());
    }
}