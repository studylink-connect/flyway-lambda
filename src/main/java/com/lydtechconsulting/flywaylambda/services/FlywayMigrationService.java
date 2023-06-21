package com.lydtechconsulting.flywaylambda.services;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.CleanResult;
import org.flywaydb.core.api.output.MigrateResult;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import static java.util.Objects.requireNonNull;

public class FlywayMigrationService {
    Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .create();

    public void performMigration(
            LambdaLogger logger,
            SecretsManagerClient secretsClient,
            String flywayScriptsLocation,
            String secretName,
            String usernameKey,
            String passwordKey,
            String hostKey,
            String portKey,
            String dbName,
            String schemaName,
            String targetVersion,
            boolean doClean
    ) {
        logger.log("performing Flyway migrate");

        JsonObject secretValue = getSecret(secretsClient, secretName);
        secretsClient.close();

        String dbPassword = secretValue.get(passwordKey).getAsString();
        String dbUser = secretValue.get(usernameKey).getAsString();
        String dbHost = secretValue.get(hostKey).getAsString();
        String dbPort = secretValue.get(portKey).getAsString();

        String dbUrl = String.format("jdbc:postgresql://%s:%s/%s", dbHost, dbPort, dbName);

        Flyway flyway = Flyway
                .configure()
                .schemas(schemaName)
                .target(targetVersion)
                .envVars() //Configures Flyway using FLYWAY_* environment variables.
                .dataSource(dbUrl, dbUser, dbPassword)
                .locations(flywayScriptsLocation)
                .load();

        if (doClean) {
            CleanResult cleanResult = flyway.clean();
            logger.log("cleaned " + cleanResult.schemasCleaned.size() + " schemas in db " + cleanResult.database + ". " +
                    "warnings: " + cleanResult.warnings.size());
        }

        MigrateResult result = flyway.migrate();

        logger.log("applied " + result.migrations.size() + " migrations to db " + result.database + ". warnings: " + result.warnings.size());
    }

    public JsonObject getSecret(SecretsManagerClient secretsClient, String secretName) {
        try {
            GetSecretValueRequest valueRequest = GetSecretValueRequest.builder()
                    .secretId(secretName)
                    .build();

            GetSecretValueResponse valueResponse = secretsClient.getSecretValue(valueRequest);
            return new Gson().fromJson(valueResponse.secretString(), JsonObject.class);
        } catch (SecretsManagerException e) {
            throw new RuntimeException("Problem getting secret:  " + secretName, e);
        }
    }
}

