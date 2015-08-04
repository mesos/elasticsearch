package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.util.ProtoTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.NotSerializableException;
import java.security.InvalidParameterException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Tests
 */
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
        status = new ESTaskStatus(state, frameworkID, taskInfo);
        taskStatus = status.getDefaultStatus();
    }

    @Test(expected = IllegalStateException.class)
    public void testHandleSetException() throws IllegalStateException, NotSerializableException {
        doThrow(NotSerializableException.class).when(state).set(anyString(), any());
        status.setStatus(taskStatus);
    }

    @Test(expected = IllegalStateException.class)
    public void testHandleGetException() throws IllegalStateException, NotSerializableException {
        doThrow(NotSerializableException.class).when(state).get(anyString());
        status.getStatus();
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptIfStateIsNull() {
        new ESTaskStatus(null, frameworkID, taskInfo);
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptIfFrameworkIDIsNull() {
        new ESTaskStatus(state, null, taskInfo);
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptIfFrameworkIDIsEmpty() {
        new ESTaskStatus(state, Protos.FrameworkID.newBuilder().setValue("").build(), taskInfo);
    }

    @Test
    public void shouldAllowNullTaskInfo() {
        new ESTaskStatus(state, frameworkID, null);
    }

    @Test
    public void shouldPrintOk() throws NotSerializableException {
        when(state.get(anyString())).thenReturn(status.getDefaultStatus());
        String s = status.toString();
        Assert.assertTrue(s.contains(Protos.TaskState.TASK_STARTING.toString()));
        Assert.assertTrue(s.contains(ESTaskStatus.DEFAULT_STATUS_NO_MESSAGE_SET));
    }

    @Test
    public void shouldHandleNoMessageError() {
        status.toString();
    }
}