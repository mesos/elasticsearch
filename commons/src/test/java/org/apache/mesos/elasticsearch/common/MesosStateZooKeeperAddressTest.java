package org.apache.mesos.elasticsearch.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for Mesos state zookeeper address parsing
 */
public class MesosStateZooKeeperAddressTest {

    @Test(expected = ZooKeeperAddressException.class)
    public void shouldExceptionIfInvalidZKAddress() {
        new ZooKeeperAddress("blah");
    }

    @Test(expected = ZooKeeperAddressException.class)
    public void shouldExceptionIfNoZKPrefix() {
        new ZooKeeperAddress("file://192.168.0.1");
    }

    @Test(expected = ZooKeeperAddressException.class)
    public void shouldExceptionIfNoAddress() {
        new ZooKeeperAddress("zk://");
    }

    @Test(expected = ZooKeeperAddressException.class)
    public void shouldExceptionIfNoPort1() {
        new ZooKeeperAddress("zk://192.168.0.1");
    }

    @Test(expected = ZooKeeperAddressException.class)
    public void shouldExceptionIfNoPort2() {
        new ZooKeeperAddress("zk://192.168.0.1:");
    }

    @Test
    public void shouldReturnInCorrectFormat() {
        String add = "zk://192.168.0.1:2182";
        String expectedResult = "192.168.0.1:2182";
        MesosStateZooKeeperAddress zk = new MesosStateZooKeeperAddress(add);
        assertEquals(expectedResult, zk.getAddress());
    }

    @Test
    public void shouldReturnInCorrectFormatWithPath() {
        String add = "zk://192.168.0.1:2182/mesos";
        String expectedResult = "192.168.0.1:2182";
        MesosStateZooKeeperAddress zk = new MesosStateZooKeeperAddress(add);
        assertEquals(expectedResult, zk.getAddress());
    }

    @Test
    public void shouldReturnMultiAddr() {
        String add = "zk://192.168.0.1:2182/mesos,192.168.0.2:2182";
        String expectedResult = "192.168.0.1:2182,192.168.0.2:2182";
        MesosStateZooKeeperAddress zk = new MesosStateZooKeeperAddress(add);
        assertEquals(expectedResult, zk.getAddress());
    }
}