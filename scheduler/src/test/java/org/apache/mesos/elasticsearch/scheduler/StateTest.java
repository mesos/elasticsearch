package org.apache.mesos.elasticsearch.scheduler;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.Protos;
import org.apache.mesos.state.Variable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;

/**
 * Tests State class.
 */
public class StateTest {
    private State state;

    @SuppressWarnings("unchecked")
    @Before
    public void before() throws ExecutionException, InterruptedException {
        ZooKeeperStateInterface zkState = Mockito.mock(ZooKeeperStateInterface.class);
        Future future = Mockito.mock(Future.class);
        Mockito.when(future.get()).thenReturn(new TestVariable());
        Mockito.when(zkState.store(any(Variable.class))).thenReturn(future);
        Mockito.when(zkState.fetch(Mockito.matches(".*/$"))).thenThrow(java.util.concurrent.ExecutionException.class);
        Mockito.when(zkState.fetch(any())).thenReturn(future);
        state = new State(zkState);
    }

    @Test
    public void testSanityCheck() {
        assertNotNull("State should not be null.", state);
    }

    @Test
    public void testInitialGetFrameworkID() throws InterruptedException, ExecutionException, InvalidProtocolBufferException {
        assertTrue("FrameworkID should be empty if not first time.", state.getFrameworkID().getValue().isEmpty());
    }

    @Test
    public void testThatStoreFrameworkIDStores() throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        Protos.FrameworkID frameworkID = Protos.FrameworkID.newBuilder().setValue("TEST_ID").build();
        state.setFrameworkId(frameworkID);
        assertNotNull("FramekworkID should not be null once set.", state.getFrameworkID());
        assertEquals("FramekworkID should be equal to the one set.", state.getFrameworkID().getValue(), frameworkID.getValue());
    }

    private static class TestVariable extends Variable {
        private byte[] myByte = new byte[0];

        @Override
        public byte[] value() {
            return myByte;
        }

        @Override
        public Variable mutate(byte[] value) {
            myByte = value;
            return this;
        }
    }

    @Test
    public void testMkDirJustSlashShouldNotCrash() throws InterruptedException, ExecutionException, ClassNotFoundException, IOException {
        state.mkdir("/");
    }

    @Test(expected = Exception.class)
    public void testMkDirTrailingSlash() throws InterruptedException, ExecutionException, ClassNotFoundException, IOException {
        state.mkdir("/mesos/");
    }

    @Test
    public void testMkDirOk() throws InterruptedException, ExecutionException, ClassNotFoundException, IOException {
        state.mkdir("/mesos");
    }
}