package org.apache.mesos.elasticsearch.scheduler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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

            File file = new File (Binaries.ES_EXECUTOR_JAR);
            t.sendResponseHeaders(200, file.length());
            writeFile(t, file);
        }
    }

    static class ZipHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {

            Headers h = t.getResponseHeaders();
            h.add("Content-Type", "application/octet-stream");

            File file = new File ("elasticsearch-cloud-mesos.zip");
            t.sendResponseHeaders(200, file.length());
            writeFile(t, file);
        }
    }

    private static void writeFile(HttpExchange t, File file) throws IOException {
        FileInputStream fis = new FileInputStream(file);

        OutputStream os = t.getResponseBody();
        IOUtils.copy(fis, os);
        os.flush();

        os.close();
        fis.close();
    }

}