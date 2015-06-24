package org.apache.mesos.elasticsearch.common;

import org.junit.Ignore;
import org.junit.Test;

import java.security.InvalidParameterException;

import static org.junit.Assert.assertEquals;

public class ZooKeeperAddressTest {
    @Test(expected = InvalidParameterException.class)
    public void shouldExceptionIfInvalidZKAddress() {
        new ZooKeeperAddress("blah");
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptionIfNoZKPrefix() {
        new ZooKeeperAddress("file://192.168.0.1");
    }

    @Test(expected = InvalidParameterException.class)
    public void shouldExceptionIfNoAddress() {
        new ZooKeeperAddress("zk://");
    }

    @Test
    public void shouldAcceptIfNoPort() {
        String add = "zk://192.168.0.1";
        ZooKeeperAddress zk = new ZooKeeperAddress(add);
        assertEquals(add, zk.getZkAddress());
        assertEquals("192.168.0.1", zk.getIpPort());
        assertEquals("", zk.getZKNode());
    }

    @Test
    public void shouldAcceptIfSingleZKAddress() {
        String add = "zk://192.168.0.1:2182";
        ZooKeeperAddress zk = new ZooKeeperAddress(add);
        assertEquals(add, zk.getZkAddress());
        assertEquals("192.168.0.1:2182", zk.getIpPort());
        assertEquals("", zk.getZKNode());
    }

    @Test
    public void shouldAcceptIfSingleZKAddressWithPath() {
        String add = "zk://192.168.0.1:2182/mesos";
        ZooKeeperAddress zk = new ZooKeeperAddress(add);
        assertEquals(add, zk.getZkAddress());
        assertEquals("192.168.0.1:2182", zk.getIpPort());
        assertEquals("/mesos", zk.getZKNode());
    }

    @Test
    public void shouldAcceptIfMultiZKAddressWithPath() {
        String add = "zk://192.168.0.1:2182,10.4.52.3:1234/mesos";
        ZooKeeperAddress zk = new ZooKeeperAddress(add);
        assertEquals(add, zk.getZkAddress());
        assertEquals("192.168.0.1:2182,10.4.52.3:1234", zk.getIpPort());
        assertEquals("/mesos", zk.getZKNode());
    }

    @Ignore
    @Test
    public void shouldAcceptIfSpacesInPath() {
        String add = "zk://192.168.0.1:2182, 10.4.52.3:1234/mesos";
        ZooKeeperAddress zk = new ZooKeeperAddress(add);
        assertEquals(add, zk.getZkAddress());
        assertEquals("192.168.0.1:2182, 10.4.52.3:1234", zk.getIpPort());
        assertEquals("/mesos", zk.getZKNode());
    }

    @Ignore
    @Test
    public void shouldAcceptIfSpacesAtEnd() {
        String add = "zk://192.168.0.1:2182 ";
        ZooKeeperAddress zk = new ZooKeeperAddress(add);
        assertEquals(add, zk.getZkAddress());
        assertEquals("192.168.0.1:2182 ", zk.getIpPort());
        assertEquals("/mesos", zk.getZKNode());
    }

    // Note this test does not work because of the regex.
    @Ignore
    @Test
    public void shouldNotWorkIfMultiZKAddressWithMultiPath() {
        String add = "zk://192.168.0.1:2182/somePath,10.4.52.3:1234/mesos";
        ZooKeeperAddress zk = new ZooKeeperAddress(add);
        assertEquals(add, zk.getZkAddress());
        assertEquals("192.168.0.1:2182,10.4.52.3:1234", zk.getIpPort());
        assertEquals("/somePath,/mesos", zk.getZKNode());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldExceptionWithMalformedAddress() {
        new ZooKeeperAddress("zk://inval?iod");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldExceptionWithMalformedPort() {
        new ZooKeeperAddress("zk://master:invalid");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldExceptionWithMalformedPortOnSecond() {
        new ZooKeeperAddress("zk://master:2130,slave:in");
    }
}