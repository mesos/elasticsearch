package org.apache.mesos.elasticsearch.scheduler;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.IOUtils;
import org.apache.mesos.elasticsearch.common.Binaries;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * Simple file server for distributing jars and zips across the cluster
 */
public class SimpleFileServer {
    private HttpServer server;

    private static void writeClassPathResource(HttpExchange t, String classPathResource) throws IOException {
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

    public InetSocketAddress getAddress() {
        if (server != null) {
            return server.getAddress();
        } else {
            return null;
        }
    }

    static class InfoHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            String response = "Use /get to download the executor jar";
            t.sendResponseHeaders(200, response.length());

            OutputStream os = t.getResponseBody();
            IOUtils.write(response, os);
            os.close();
        }
    }

    static class GetHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {

            Headers h = t.getResponseHeaders();
            h.add("Content-Type", "application/octet-stream");

            writeClassPathResource(t, Binaries.ES_EXECUTOR_JAR);
        }
    }

}