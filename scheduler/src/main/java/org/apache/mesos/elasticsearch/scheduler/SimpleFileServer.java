package org.apache.mesos.elasticsearch.scheduler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.Headers;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.common.Binaries;
import org.apache.mesos.elasticsearch.common.Configuration;
import org.apache.mesos.elasticsearch.common.HostResolver;

/**
 * Simple file server for distributing jars and zips across the cluster
 */
public class SimpleFileServer {

    public static final Logger LOGGER = Logger.getLogger(ElasticsearchScheduler.class.toString());

    public static final int HTTP_OK = 200;
    public static final String EXECUTOR_ENDPOINT = "/" + Binaries.ES_EXECUTOR_JAR;
    public static final String CLOUD_MESOS_ENDPOINT = "/" + Binaries.ES_CLOUD_MESOS_ZIP;

    private HttpServer server;

    private final String baseUrl;

    public static void main(String[] args) {
        SimpleFileServer simpleFileServer = new SimpleFileServer(Configuration.FILE_SERVER_PORT);
        simpleFileServer.start();
    }

    public SimpleFileServer(int port) {
        try {
            InetAddress inetAddress = HostResolver.resolve(InetAddress.getLocalHost().getHostName());
            this.baseUrl = "http://" + inetAddress.getHostAddress() + ":" + Configuration.FILE_SERVER_PORT;
        } catch (UnknownHostException e) {
            LOGGER.error("Could not determine hostname");
            throw new RuntimeException("Could not determine Elasticsearch file server hostname");
        }

        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            LOGGER.error("Could not start Elasticsearch file server", e);
            throw new RuntimeException("Could not start Elasticsearch file server");
        }
    }

    public void start() {
        LOGGER.info("Starting Elasticsearch file server on " + baseUrl);

        server.createContext("/info", new InfoHandler());
        server.createContext(EXECUTOR_ENDPOINT, new ExecutorJarHandler());
        server.createContext(CLOUD_MESOS_ENDPOINT, new CloudMesosHandler());
        server.setExecutor(null); // creates a default executor
        server.start();
    }

    public void stop() {
        server.stop(0);
    }

    public String getExecutorJarUrl() {
        return baseUrl + EXECUTOR_ENDPOINT;
    }

    public String getCloudMesosUrl() {
        return baseUrl + CLOUD_MESOS_ENDPOINT;
    }

    static class InfoHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            String response = "Use " +  EXECUTOR_ENDPOINT + " to download the executor jar and " + CLOUD_MESOS_ENDPOINT + " to download cloud-mesos zip\n";
            t.sendResponseHeaders(HTTP_OK, response.length());

            OutputStream os = t.getResponseBody();
            IOUtils.write(response, os);
            os.close();
        }
    }

    static class ExecutorJarHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            Headers h = t.getResponseHeaders();
            h.add("Content-Type", "application/octet-stream");

            writeFile(t, Binaries.ES_EXECUTOR_JAR);
        }
    }

    static class CloudMesosHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            Headers h = t.getResponseHeaders();
            h.add("Content-Type", "application/octet-stream");

            writeFile(t, Binaries.ES_CLOUD_MESOS_ZIP);
        }
    }

    private static void writeFile(HttpExchange t, String fileName) {
        File file = new File(System.getProperty("user.dir"), fileName);

        try (FileInputStream fis = new FileInputStream(file)) {
            OutputStream os = t.getResponseBody();
            t.sendResponseHeaders(HTTP_OK, file.length());
            IOUtils.copy(fis, os);
            os.flush();

            os.close();
            fis.close();
        } catch (IOException e) {
            LOGGER.error("Could not write file '" + fileName + "'", e);
        }
    }

}