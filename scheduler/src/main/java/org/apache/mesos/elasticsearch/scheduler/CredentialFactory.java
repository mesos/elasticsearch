package org.apache.mesos.elasticsearch.scheduler;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;

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
                String secretData = IOUtils.toString(new FileInputStream(secretFilePath), Charset.defaultCharset());
                credentialBuilder.setSecret(secretData);
            } catch (IOException cause) {
                LOGGER.error("Error reading authentication secret from file: " + secretFilePath, cause);
            }
        }
        return credentialBuilder;
    }
}