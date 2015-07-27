package org.apache.mesos.elasticsearch.scheduler;

import org.junit.Test;

/**
 * Main test
 */
public class MainTest {
    @Test(expected = RuntimeException.class)
    public void testErrorIfNoHeap() throws Exception {
        Main.main(new String[0]);
    }
}