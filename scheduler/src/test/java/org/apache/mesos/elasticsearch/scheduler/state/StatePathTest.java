package org.apache.mesos.elasticsearch.scheduler.state;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNotNull;

/**
 * Tests State class.
 */
public class StatePathTest {
    private StatePath statePath;

    @Before
    public void before() throws ExecutionException, InterruptedException {
        statePath = new StatePath(new TestSerializableStateImpl());
    }

    @Test
    public void testSanityCheck() {
        assertNotNull("State should not be null.", statePath);
    }

    @Test
    public void testMkDirJustSlashShouldNotCrash() throws InterruptedException, ExecutionException, ClassNotFoundException, IOException {
        statePath.mkdir("/");
    }

    @Test(expected = Exception.class)
    public void testMkDirTrailingSlash() throws InterruptedException, ExecutionException, ClassNotFoundException, IOException {
        statePath.mkdir("/mesos/");
    }

    @Test
    public void testMkDirOk() throws InterruptedException, ExecutionException, ClassNotFoundException, IOException {
        statePath.mkdir("/mesos");
    }

}