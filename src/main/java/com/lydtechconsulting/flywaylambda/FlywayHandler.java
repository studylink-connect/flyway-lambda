package com.lydtechconsulting.flywaylambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.lydtechconsulting.flywaylambda.services.FileDownloadService;
import com.lydtechconsulting.flywaylambda.services.FlywayMigrationService;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class FlywayHandler implements RequestHandler<Map<String, String>, String> {
    Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .create();

    private FlywayMigrationService flywayMigrationService = new FlywayMigrationService();
    private FileDownloadService fileDownloadService = new FileDownloadService();

    @Override
    public String handleRequest(Map<String, String> event, Context context) {
        LambdaLogger logger = context.getLogger();
        logDetails(event, context, logger);

        String bucketName = getStringParam(event, "bucket_name");
        String secretName = getStringParam(event, "secret_name");
        String dbName = getStringParam(event, "database_name");
        String schemaName = getStringParam(event, "schema_name");
        String usernameKey = getStringParam(event, "username_key", "username");
        String passwordKey = getStringParam(event, "password_key", "password");
        String hostKey = getStringParam(event, "host_key", "host");
        String portKey = getStringParam(event, "port_key", "port");
        String targetVersion = getStringParam(event, "target_version", "latest");
        boolean doClean = Boolean.parseBoolean(getStringParam(event, "do_clean", "false"));

        String flywayScriptsLocation = "/tmp/sqlFiles_" + System.currentTimeMillis();
        logger.log("bucketName: " + bucketName);
        logger.log("destination: " + flywayScriptsLocation);
        
        String regionString = System.getenv("AWS_REGION");
        requireNonNull(regionString, "AWS_REGION expected to be set");
        Region region = Region.of(regionString);
        
        createDirectory(flywayScriptsLocation);
        final S3Client s3Client = S3Client.builder()
                .region(region) 
                .build();
        fileDownloadService.copy(logger, s3Client, bucketName, flywayScriptsLocation);

        SecretsManagerClient secretsClient = SecretsManagerClient.builder()
                .region(region)
                .build();
        flywayMigrationService.performMigration(logger, secretsClient, "filesystem://" + flywayScriptsLocation, secretName,
                usernameKey, passwordKey, hostKey, portKey, dbName, schemaName, targetVersion, doClean);

        return "200 OK";
    }

    private String getStringParam(Map<String, String> event, String paramName) {
        return getStringParam(event, paramName, null);
    }

    private String getStringParam(Map<String, String> event, String paramName, String defaultValue) {
        if (!event.containsKey(paramName)) {
            if (defaultValue != null) {
                return defaultValue;
            }
            throw new NullPointerException(String.format("event must have %s field", paramName));
        }
        return event.get(paramName);
    }

    private void createDirectory(String flywayScriptsLocation) {
        try {
            Files.createDirectories(Paths.get(flywayScriptsLocation));
        } catch (IOException e) {
            throw new RuntimeException("problem creating directory", e);
        }
    }

    private void logDetails(Map<String, String> event, Context context, LambdaLogger logger) {
        // log execution details
        logger.log("ENVIRONMENT VARIABLES: " + gson.toJson(System.getenv()));
        logger.log("CONTEXT: " + context.toString());
        // process event
        logger.log("EVENT: " + event.toString());
    }
    
}
