package org.apache.mesos.elasticsearch.common.zookeeper.parser;

import org.apache.mesos.elasticsearch.common.zookeeper.exception.ZKAddressException;
import org.apache.mesos.elasticsearch.common.zookeeper.model.ZKAddress;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for zk Address parser
 */
@SuppressWarnings({"PMD.TooManyMethods", "PMD.AvoidUsingHardCodedIP"})
public class ZKAddressParserTest {

    @Test(expected = ZKAddressException.class)
    public void shouldExceptionIfInvalidZKAddress() {
        new ZKAddressParser().validateZkUrl("blah");
    }

    @Test(expected = ZKAddressException.class)
    public void shouldExceptionIfNoZKPrefix() {
        new ZKAddressParser().validateZkUrl("file://192.168.0.1");
    }

    @Test(expected = ZKAddressException.class)
    public void shouldExceptionIfNoAddress() {
        new ZKAddressParser().validateZkUrl("zk://");
    }

    @Test(expected = ZKAddressException.class)
    public void shouldExceptionIfNoPort() {
        String add = "zk://192.168.0.1";
        new ZKAddressParser().validateZkUrl(add);
    }

    @Test(expected = ZKAddressException.class)
    public void shouldExceptionWithMalformedAddress() {
        new ZKAddressParser().validateZkUrl("zk://inval?iod");
    }

    @Test(expected = ZKAddressException.class)
    public void shouldExceptionWithMalformedPort() {
        new ZKAddressParser().validateZkUrl("zk://master:invalid");
    }

    @Test(expected = ZKAddressException.class)
    public void shouldExceptionWithoutPath() {
        new ZKAddressParser().validateZkUrl("zk://master:2181");
    }

    @Test(expected = ZKAddressException.class)
    public void shouldExceptionWithMalformedPortOnSecond() {
        new ZKAddressParser().validateZkUrl("zk://master:2130,slave:in");
    }

    @Test
    public void shouldAcceptIfSingleZKAddressWithPath() {
        String add = "zk://192.168.0.1:2182/mesos";
        List<ZKAddress> zk = new ZKAddressParser().validateZkUrl(add);
        assertZKEquals(zk.get(0), "", "", "192.168.0.1", "2182", "mesos");
    }

    @Test
    public void shouldAcceptIfMultiZKAddressWithPath() {
        String add = "zk://192.168.0.1:2182,10.4.52.3:1234/mesos";
        List<ZKAddress> zk = new ZKAddressParser().validateZkUrl(add);
        assertZKEquals(zk.get(0), "", "", "192.168.0.1", "2182", "");
        assertZKEquals(zk.get(1), "", "", "10.4.52.3", "1234", "mesos");
    }

    @Test
    public void shouldAcceptIfSpacesInPath() {
        String add = "zk://192.168.0.1:2182, 10.4.52.3:1234/mesos";
        List<ZKAddress> zk = new ZKAddressParser().validateZkUrl(add);
        assertZKEquals(zk.get(0), "", "", "192.168.0.1", "2182", "");
        assertZKEquals(zk.get(1), "", "", "10.4.52.3", "1234", "mesos");
    }

    @Test
    public void shouldAcceptIfMultiZKAddressWithMultiPath() {
        String add = "zk://192.168.0.1:2182/somePath,10.4.52.3:1234/mesos";
        List<ZKAddress> zk = new ZKAddressParser().validateZkUrl(add);
        assertZKEquals(zk.get(0), "", "", "192.168.0.1", "2182", "somePath");
        assertZKEquals(zk.get(1), "", "", "10.4.52.3", "1234", "mesos");
    }

    @Test
    public void shouldAcceptIfUserPass() {
        String add = "zk://bob:pass@192.168.0.1:2182/mesos";
        List<ZKAddress> zk = new ZKAddressParser().validateZkUrl(add);
        assertZKEquals(zk.get(0), "bob", "pass", "192.168.0.1", "2182", "mesos");
    }

    @Test
    public void shouldAcceptIfMultiUserPassSpaces() {
        String add = "zk://bob:pass@192.168.0.1:2182, team:dev@10.4.52.3:1234/mesos";
        List<ZKAddress> zk = new ZKAddressParser().validateZkUrl(add);
        assertZKEquals(zk.get(0), "bob", "pass", "192.168.0.1", "2182", "");
        assertZKEquals(zk.get(1), "team", "dev", "10.4.52.3", "1234", "mesos");
    }

    private void assertZKEquals(ZKAddress zk, String user, String pass, String addr, String port, String zkNode) {
        assertEquals(addr, zk.getAddress());
        assertEquals(port, zk.getPort());
        assertEquals(user, zk.getUser());
        assertEquals(pass, zk.getPassword());
        assertEquals(zkNode, zk.getZkNode());
    }
}