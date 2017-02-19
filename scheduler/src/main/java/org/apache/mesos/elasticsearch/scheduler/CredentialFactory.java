package org.apache.mesos.elasticsearch.scheduler;

import com.google.protobuf.ByteString;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Factory to read credentials and return credential builder.
 */
public class CredentialFactory {
    private static final Logger LOGGER = Logger.getLogger(CredentialFactory.class);
    private final Configuration configuration;

    public CredentialFactory(Configuration configuration) {
        this.configuration = configuration;
    }

    Protos.Credential.Builder getBuilder() {
        Protos.Credential.Builder credentialBuilder = Protos.Credential.newBuilder();
        String principal = configuration.getFrameworkPrincipal();
        String secretFilePath = configuration.getFrameworkSecretPath();
        if (!principal.isEmpty() && !secretFilePath.isEmpty()) {
            credentialBuilder.setPrincipal(principal);
            try {
                ByteString bytes = ByteString.readFrom(new FileInputStream(secretFilePath));
                credentialBuilder.setSecretBytes(bytes);
            } catch (IOException cause) {
                LOGGER.error("Error reading authentication secret from file: " + secretFilePath, cause);
            }
        }
        return credentialBuilder;
    }
}
