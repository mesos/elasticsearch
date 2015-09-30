package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Tests
 */
public class FrameworkStateTest {

    public static final Protos.FrameworkID FRAMEWORK_ID = Protos.FrameworkID.newBuilder().setValue("FrameworkID").build();
    public final SerializableState state = mock(SerializableState.class);
    private final FrameworkState frameworkState = new FrameworkState(state);
    private SchedulerDriver driver = mock(SchedulerDriver.class);

    @Test
    public void testSetFrameworkID() throws IOException {
        frameworkState.markRegistered(FRAMEWORK_ID, driver);
        verify(state, times(1)).set(anyString(), eq(FRAMEWORK_ID));
    }

    @Test
    public void testGetFrameworkID() throws IOException {
        when(state.get(anyString())).thenReturn(FRAMEWORK_ID);
        Protos.FrameworkID frameworkID = frameworkState.getFrameworkID();
        verify(state, times(1)).get(anyString());
        assertEquals(FRAMEWORK_ID, frameworkID);
    }

    @Test
    public void testGetEmptyWhenNoFrameworkID() throws IOException {
        Protos.FrameworkID frameworkID = frameworkState.getFrameworkID();
        verify(state, times(1)).get(anyString());
        assertEquals("", frameworkID.getValue());
    }

    @Test
    public void testHandleSetException() throws IOException {
        doThrow(IOException.class).when(state).set(anyString(), any());
        frameworkState.markRegistered(FRAMEWORK_ID, driver);
    }

    @Test
    public void testHandleGetException() throws IOException {
        doThrow(IOException.class).when(state).get(anyString());
        frameworkState.getFrameworkID();
    }
}