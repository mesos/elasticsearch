package org.apache.mesos.elasticsearch.scheduler;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.common.util.NetworkUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * Simple file server for distributing jars and zips across the cluster
 */
public class SimpleFileServer implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(SimpleFileServer.class);
    private HttpServer server;
    private final String file;

    public SimpleFileServer(String file) {
        this.file = file;
    }

    private void writeClassPathResource(HttpExchange t, String classPathResource) throws IOException {
        InputStream in = SimpleFileServer.class.getClassLoader().getResourceAsStream(classPathResource);

        // Must send headers before body.
        t.sendResponseHeaders(200, 0);
        OutputStream os = t.getResponseBody();
        IOUtils.copy(in, os);
        os.flush();

        os.close();
        in.close();
    }

    public void serve() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0); // Pick a random available port
        server.createContext("/get", new GetHandler());
        server.setExecutor(null); // creates a default executor
        server.start();
        LOGGER.info("Running Executor JAR file server on: " + this.getAddress().getHostName() + ":" + this.getAddress().getPort());
    }

    public InetSocketAddress getAddress() {
        if (server == null) {
            throw new IllegalStateException("Fileserver is not running. Cannot get address.");
        } else {
            return NetworkUtils.hostSocket(server.getAddress().getPort());
        }
    }

    @Override
    public void run() {
        try {
            this.serve();
        } catch (IOException e) {
            LOGGER.error("Elasticsearch file server stopped", e);
        }
    }

    class GetHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {

            Headers h = t.getResponseHeaders();
            h.add("Content-Type", "application/octet-stream");

            writeClassPathResource(t, file);
        }
    }

}