package org.apache.mesos.elasticsearch.scheduler;

import com.google.protobuf.InvalidProtocolBufferException;
import junit.framework.Assert;
import org.apache.mesos.Protos;
import org.apache.mesos.state.Variable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Tests State class.
 */
public class StateTest {
    private State state;
    private ZooKeeperStateInterface zkState;

    @Before
    public void before() throws ExecutionException, InterruptedException {
        zkState = Mockito.mock(ZooKeeperStateInterface.class);
        Future future = Mockito.mock(Future.class);
        Mockito.when(future.get()).thenReturn(new TestVariable());
        Mockito.when(zkState.fetch(Mockito.anyString())).thenReturn(future);
        Mockito.when(zkState.store(Mockito.any(Variable.class))).thenReturn(future);
        state = new State(zkState);
    }

    @Test
    public void testSanityCheck() {
        Assert.assertNotNull("State should not be null.", state);
    }

    @Test
    public void testInitialGetFrameworkID() throws InterruptedException, ExecutionException, InvalidProtocolBufferException {
        Assert.assertNull("FrameworkID should be null if not first time.", state.getFrameworkID());
    }

    @Test
    public void testThatStoreFrameworkIDStores() throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        Protos.FrameworkID frameworkID = Protos.FrameworkID.newBuilder().setValue("TEST_ID").build();
        state.setFrameworkId(frameworkID);
        Assert.assertNotNull("FramekworkID should not be null once set.", state.getFrameworkID());
        Assert.assertEquals("FramekworkID should be equal to the one set.", state.getFrameworkID().getValue(), frameworkID.getValue());
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
}