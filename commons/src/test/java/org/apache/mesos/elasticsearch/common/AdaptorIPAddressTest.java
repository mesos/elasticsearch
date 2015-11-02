package org.apache.mesos.elasticsearch.common;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author philwinder
 * @since 21/10/2015
 */
public class AdaptorIPAddressTest {

    @Test @Ignore("Test is being taken care of at https://github.com/mesos/elasticsearch/issues/392")
    public void testEth0() throws Exception {
        assertNotNull(AdaptorIPAddress.eth0());
        assertTrue(AdaptorIPAddress.eth0().getHostAddress().contains("."));
        assertFalse(AdaptorIPAddress.eth0().getHostAddress().contains(":"));
    }
}