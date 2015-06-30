package org.apache.mesos.elasticsearch.common.zookeeper.formatter;

import org.apache.mesos.elasticsearch.common.zookeeper.model.ZKAddress;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;

/**
 * Tests parsing of Zookeeper addresses
 */
public class ZKAddressTest {
    @Test
    public void shouldReturnSameAddressIfValidated() {
        ZKAddressParser mock = Mockito.mock(ZKAddressParser.class);
        Mockito.when(mock.validateZkUrl(anyString())).thenReturn(new ArrayList<ZKAddress>(0));
        ZKFormatter formatter = new ZooKeeperFormatter(mock);
        String add = "zk://192.168.0.1:2182";
        String address = formatter.format(add);
        assertEquals(add, address);
    }
}