package org.apache.mesos.elasticsearch.scheduler;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

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
        server.createContext("/info", new InfoHandler());
        server.createContext("/get", new GetHandler());
        server.setExecutor(null); // creates a default executor
        server.start();
    }

    public InetSocketAddress getAddress() throws UnknownHostException {
        if (server != null) {
            return new InetSocketAddress(InetAddress.getLocalHost().getHostName(), server.getAddress().getPort());
        } else {
            return null;
        }
    }

    @Override
    public void run() {
        try {
            this.serve();
            LOGGER.info("Running Executor JAR file server on: " + this.getAddress().getHostName() + ":" + this.getAddress().getPort());
        } catch (IOException e) {
            LOGGER.error("Elasticsearch file server stopped", e);
            e.printStackTrace();
        }
    }

    class InfoHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            String response = "Use /get to download the executor jar";
            t.sendResponseHeaders(200, response.length());

            OutputStream os = t.getResponseBody();
            IOUtils.write(response, os);
            os.close();
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