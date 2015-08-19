package org.apache.mesos.elasticsearch.scheduler;

import com.google.protobuf.ByteString;
import org.apache.mesos.Protos;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Factory to read credentials and return credential builder.
 */
public class CredentialFactory {
    private final Configuration configuration;

    public CredentialFactory(Configuration configuration) {
        this.configuration = configuration;
    }

    Protos.Credential.Builder getBuilder() throws IOException {
        Protos.Credential.Builder credentialBuilder = Protos.Credential.newBuilder();
        String principal = configuration.getMesosAuthenticationPrincipal();
        String secretFilePath = configuration.getMesosAuthenticationSecretFile();
        if (!principal.isEmpty() && !secretFilePath.isEmpty()) {
            credentialBuilder.setPrincipal(principal);
            try {
                ByteString bytes = ByteString.readFrom(new FileInputStream(secretFilePath));
                credentialBuilder.setSecret(bytes);
            } catch (IOException cause) {
                throw new IOException("Error reading authentication secret from file: " + secretFilePath, cause);
            }
        }
        return credentialBuilder;
    }
}