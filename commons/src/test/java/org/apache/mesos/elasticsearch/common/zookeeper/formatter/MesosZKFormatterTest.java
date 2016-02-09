package org.apache.mesos.elasticsearch.common.zookeeper.formatter;

import org.apache.mesos.elasticsearch.common.zookeeper.model.ZKAddress;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Tests for Mesos ZK formatting. Basically the same as MesosStateZKFormatter, but with /mesos
 */
public class MesosZKFormatterTest {

    private ZKAddressParser parser;

    private MesosZKFormatter formatter;

    @Before
    public void before() {
        parser = Mockito.mock(ZKAddressParser.class);
        formatter = new MesosZKFormatter(parser);
    }

    @Test
    public void testFormat() {
        String zkUrl = "zk://zookeeper:2181/mesos";

        when(parser.validateZkUrl(zkUrl)).thenReturn(Collections.singletonList(
                new ZKAddress("zookeeper:2181")
        ));

        String format = formatter.format(zkUrl);

        assertEquals("zk://zookeeper:2181/mesos", format);
    }

    @Test
    public void testFormat_threeNodes() {
        String zkUrl = "zk://zookeeper1:2181,zookeeper2:2181,zookeeper3:2181/mesos";

        when(parser.validateZkUrl(zkUrl)).thenReturn(asList(
                new ZKAddress("zookeeper1:2181"),
                new ZKAddress("zookeeper2:2181"),
                new ZKAddress("zookeeper3:2181")
        ));

        String format = formatter.format(zkUrl);

        assertEquals("zk://zookeeper1:2181,zookeeper2:2181,zookeeper3:2181/mesos", format);
    }

    @Test
    public void testFormat_shouldReturnSameAddressIfValidated() {
        String add = "192.168.0.1:2182";
        List<ZKAddress> dummyReturn = new ArrayList<>(1);
        dummyReturn.add(new ZKAddress(add));
        when(parser.validateZkUrl(anyString())).thenReturn(dummyReturn);
        String address = formatter.format(""); // Doesn't matter. We're returning a dummy.
        assertEquals(ZKAddress.ZK_PREFIX + add + MesosZKFormatter.MESOS_PATH, address);
    }

    @Test
    public void testFormat_shouldReturnMultiAddress() {
        String add1 = "192.168.0.1:2182";
        String add2 = "192.168.0.2:2183";
        String concat = add1 + "," + add2;
        List<ZKAddress> dummyReturn = new ArrayList<>(1);
        dummyReturn.add(new ZKAddress(add1));
        dummyReturn.add(new ZKAddress(add2));
        when(parser.validateZkUrl(anyString())).thenReturn(dummyReturn);
        String address = formatter.format(""); // Doesn't matter. We're returning a dummy.
        assertEquals(ZKAddress.ZK_PREFIX + concat + MesosZKFormatter.MESOS_PATH, address);
    }

    @Test
    public void testFormat_shouldReturnArrayWhenUsersOrPath() {
        String add1 = "192.168.0.1:2182";
        String add2 = "192.168.0.2:2183";
        String concat = add1 + "," + add2;
        List<ZKAddress> dummyReturn = new ArrayList<>(1);
        dummyReturn.add(new ZKAddress(add1 + "/mesos"));
        dummyReturn.add(new ZKAddress("bob:pass@" + add2));
        when(parser.validateZkUrl(anyString())).thenReturn(dummyReturn);
        String address = formatter.format(""); // Doesn't matter. We're returning a dummy.
        assertEquals(ZKAddress.ZK_PREFIX + concat + MesosZKFormatter.MESOS_PATH, address);
    }
}