package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.mesos.Protos;
import org.apache.mesos.state.Variable;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.NotSerializableException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * Tests State class.
 */
public class StateTest {
    private State state;

    @Before
    public void before() throws ExecutionException, InterruptedException {
        state = new State(new TestSerializableStateImpl());
    }

    @Test
    public void testSanityCheck() {
        assertNotNull("State should not be null.", state);
    }

    @Test
    public void testInitialGetFrameworkID() throws NotSerializableException {
        assertTrue("FrameworkID should be empty if not first time.", state.getFrameworkID().getValue().isEmpty());
    }

    @Test
    public void testThatStoreFrameworkIDStores() throws NotSerializableException {
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