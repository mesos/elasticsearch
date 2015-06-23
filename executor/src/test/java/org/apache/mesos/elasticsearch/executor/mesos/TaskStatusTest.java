package org.apache.mesos.elasticsearch.executor.mesos;

import org.apache.mesos.Protos;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests
 */
public class TaskStatusTest {
    private TaskStatus status = new TaskStatus();
    @Test(expected = NullPointerException.class)
    public void shouldExceptionIfPassedNull() {
        status.setTaskID(null);
    }

    @Test
    public void shouldReturnValidProtos() {
        status.setTaskID(Protos.TaskID.newBuilder().setValue("").build());
        assertNotNull(status.starting());
        assertNotNull(status.running());
        assertNotNull(status.finished());
        assertNotNull(status.failed());
        assertNotNull(status.error());
    }
}