package org.apache.mesos.elasticsearch.scheduler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.Headers;
import org.apache.commons.io.IOUtils;
import org.apache.mesos.elasticsearch.common.Binaries;

/**
 * Simple file server for distributing jars and zips across the cluster
 */
public class SimpleFileServer {

    public void serve() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
        server.createContext("/info", new InfoHandler());
        server.createContext("/get", new GetHandler());
        server.createContext("/zip", new ZipHandler());
        server.setExecutor(null); // creates a default executor
        server.start();
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

    static class ZipHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {

            Headers h = t.getResponseHeaders();
            h.add("Content-Type", "application/octet-stream");

            writeClassPathResource(t, Binaries.ES_CLOUD_MESOS_ZIP);
        }
    }

    private static void writeClassPathResource(HttpExchange t, String classPathResource) throws IOException {
        InputStream in = SimpleFileServer.class.getClassLoader().getResourceAsStream(classPathResource);

        OutputStream os = t.getResponseBody();
        int length = IOUtils.copy(in, os);
        t.sendResponseHeaders(200, length);
        os.flush();

        os.close();
        in.close();
    }

}