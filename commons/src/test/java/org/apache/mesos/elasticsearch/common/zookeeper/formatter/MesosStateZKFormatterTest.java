package org.apache.mesos.elasticsearch.common.zookeeper.formatter;

import org.apache.mesos.elasticsearch.common.zookeeper.model.ZKAddress;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;

/**
 * Tests for Mesos state zookeeper address parsing
 */
public class MesosStateZKFormatterTest {
    @Test
    public void shouldReturnSameAddressIfValidated() {
        ZKAddressParser mock = Mockito.mock(ZKAddressParser.class);
        String add = "192.168.0.1:2182";
        List<ZKAddress> dummyReturn = new ArrayList<>(1);
        dummyReturn.add(new ZKAddress(add));
        Mockito.when(mock.validateZkUrl(anyString())).thenReturn(dummyReturn);
        ZKFormatter formatter = new MesosStateZKFormatter(mock);
        String address = formatter.format(""); // Doesn't matter. We're returning a dummy.
        assertEquals(add, address);
    }

    @Test
    public void shouldReturnMultiAddress() {
        ZKAddressParser mock = Mockito.mock(ZKAddressParser.class);
        String add1 = "192.168.0.1:2182";
        String add2 = "192.168.0.2:2183";
        String concat = add1 + "," + add2;
        List<ZKAddress> dummyReturn = new ArrayList<>(1);
        dummyReturn.add(new ZKAddress(add1));
        dummyReturn.add(new ZKAddress(add2));
        Mockito.when(mock.validateZkUrl(anyString())).thenReturn(dummyReturn);
        ZKFormatter formatter = new MesosStateZKFormatter(mock);
        String address = formatter.format(""); // Doesn't matter. We're returning a dummy.
        assertEquals(concat, address);
    }

    @Test
    public void shouldReturnArrayWhenUsersOrPath() {
        ZKAddressParser mock = Mockito.mock(ZKAddressParser.class);
        String add1 = "192.168.0.1:2182";
        String add2 = "192.168.0.2:2183";
        String concat = add1 + "," + add2;
        List<ZKAddress> dummyReturn = new ArrayList<>(1);
        dummyReturn.add(new ZKAddress(add1 + "/mesos"));
        dummyReturn.add(new ZKAddress("bob:pass@" + add2));
        Mockito.when(mock.validateZkUrl(anyString())).thenReturn(dummyReturn);
        ZKFormatter formatter = new MesosStateZKFormatter(mock);
        String address = formatter.format(""); // Doesn't matter. We're returning a dummy.
        assertEquals(concat, address);
    }
}