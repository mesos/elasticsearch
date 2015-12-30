package org.apache.mesos.elasticsearch.scheduler;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 */
public class SimpleFileServerTest {
    public static final String TEST_FILE = "test.file";
    // TODO (pnw): Shouldn't have to do this. Refactor configuration.
    private Configuration configuration = new Configuration(ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, "dummy");

    @Test
    public void shouldStartAndServeFile() throws UnknownHostException, UnirestException, InterruptedException {
        final SimpleFileServer simpleFileServer = new SimpleFileServer(configuration, TEST_FILE);
        simpleFileServer.run();
        InetSocketAddress address = simpleFileServer.getAddress();
        String serverAddress = configuration.addressToString(address);
        HttpResponse<String> response = Unirest.get(serverAddress + "/get").asString();
        assertEquals(200, response.getStatus());
        assertTrue(response.getBody().contains("This is a test file"));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldErrorIfGettingAddressBeforeStart() {
        final SimpleFileServer simpleFileServer = new SimpleFileServer(configuration, TEST_FILE);
        simpleFileServer.getAddress();
    }
}