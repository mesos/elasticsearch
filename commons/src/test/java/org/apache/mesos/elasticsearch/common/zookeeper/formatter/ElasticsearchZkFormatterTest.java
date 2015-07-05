package org.apache.mesos.elasticsearch.common.zookeeper.formatter;

import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 *
 */
public class ElasticsearchZkFormatterTest {

    @Test
    public void shouldReturnSameAddressIfValidated() {
        ZKAddressParser mock = mock(ZKAddressParser.class);

        ZKFormatter formatter = new ElasticsearchZKFormatter(mock);

        String zkUrl = "zk://host1:port1,host2:port2/mesos";

        String address = formatter.format(zkUrl);

        assertEquals("host1:port1,host2:port2/mesos", address);

        verify(mock).validateZkUrl(zkUrl);
    }

}
