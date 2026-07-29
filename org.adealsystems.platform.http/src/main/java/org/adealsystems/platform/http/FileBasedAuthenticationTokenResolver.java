/*
 * Copyright 2020-2025 ADEAL Systems GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.adealsystems.platform.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FileBasedAuthenticationTokenResolver implements AuthenticationTokenResolver {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedAuthenticationTokenResolver.class);

    private static final int MAX_RETRIES = 5;
    private static final int RETRY_DELAY = 10_000;
    private static final Duration REQUEST_TIMEOUT = Duration.ofMinutes(5);

    private final String persistentAuthTokenFile;
    private final String authServiceUrl;

    // private static final String AUTH_TOKEN_REQUEST_PAYLOAD = "client_id=gateway&grant_type=password&username=${username}&password=${password}";
    private final String authTokenRequestPayload;
    private final String username;
    private final String password;
    private final ObjectMapper objectMapper;
    private final Path persistentAuthTokenPath;
    private final Path persistentAuthTokenLockPath;

    private String token;

    public FileBasedAuthenticationTokenResolver(
        String persistentAuthTokenFile,
        String authServiceUrl,
        String authTokenRequestPayload,
        String username,
        String password,
        ObjectMapper objectMapper
    ) {
        this.persistentAuthTokenFile = persistentAuthTokenFile;
        this.persistentAuthTokenPath = Paths.get(persistentAuthTokenFile);
        this.persistentAuthTokenLockPath = Paths.get(persistentAuthTokenFile + ".lock");
        this.authServiceUrl = authServiceUrl;
        this.authTokenRequestPayload = authTokenRequestPayload;
        this.username = URLEncoder.encode(username, StandardCharsets.ISO_8859_1);
        this.password = URLEncoder.encode(password, StandardCharsets.ISO_8859_1);
        this.objectMapper = objectMapper;
    }

    @Override
    public synchronized String resolveToken(HttpClient client) {
        if (token == null) {
            token = loadAuthTokenFromFile();
        }

        if (token == null) {
            token = refreshToken(client, null);
        }
        return token;
    }

    @Override
    public synchronized String resolveToken(CloseableHttpClient client) {
        if (token == null) {
            token = loadAuthTokenFromFile();
        }

        if (token == null) {
            token = refreshToken(client, null);
        }
        return token;
    }

    private String loadAuthTokenFromFile() {
        if (!Files.exists(persistentAuthTokenPath)) {
            LOGGER.info("Token file '{}' does not exist", persistentAuthTokenFile);
            return null;
        }

        LOGGER.info("Token file '{}' exists, validating the token", persistentAuthTokenFile);
        try {
            String loadedToken = Files.readString(persistentAuthTokenPath, StandardCharsets.UTF_8).trim();
            return loadedToken.isEmpty() ? null : loadedToken;
        }
        catch (IOException ex) {
            LOGGER.error("Failed to read token file '{}'", persistentAuthTokenFile, ex);
            return null;
        }
    }

    private void storeAuthToken(String tokenToStore) {
        LOGGER.info("Storing token to file '{}'", persistentAuthTokenFile);
        try {
            Path parentPath = persistentAuthTokenPath.getParent();
            if (parentPath != null) {
                Files.createDirectories(parentPath);
            }

            Path tempFile = Files.createTempFile(
                parentPath != null ? parentPath : persistentAuthTokenPath.toAbsolutePath().getParent(),
                persistentAuthTokenPath.getFileName().toString(),
                ".tmp"
            );
            Files.writeString(
                tempFile,
                tokenToStore,
                StandardCharsets.UTF_8,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
            );
            Files.move(
                tempFile,
                persistentAuthTokenPath,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
            );
        }
        catch (IOException ex) {
            LOGGER.error("Could not store auth token", ex);
        }
    }

    @SuppressWarnings({"PMD.ExceptionAsFlowControl", "PMD.AvoidInstantiatingObjectsInLoops", "PMD.PreserveStackTrace"})
    private String requestAuthToken(HttpClient client) {
        String requestPayload = StringSubstitutor.replace(
            authTokenRequestPayload,
            Map.of(
                "username", username,
                "password", password
            )
        );

        LOGGER.info("Requesting auth token via HttpClient");
        for (int retryCounter = 0; retryCounter < MAX_RETRIES; retryCounter++) {
            if (retryCounter > 0) {
                waitBeforeRetry();
                LOGGER.debug("Retrying {}/{} on HttpClient", retryCounter, MAX_RETRIES);
            }

            try {
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(authServiceUrl))
                    .timeout(REQUEST_TIMEOUT)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(requestPayload))
                    .build();

                HttpResponse<String> response;
                CompletableFuture<HttpResponse<String>> future = client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
                try {
                    response = future.get(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException ex) {
                    future.cancel(true);
                    throw ex;
                }
                catch (TimeoutException ex) {
                    future.cancel(true);
                    HttpTimeoutException timeout = new HttpTimeoutException("Request timed out after " + REQUEST_TIMEOUT);
                    timeout.initCause(ex);
                    throw timeout;
                }
                catch (ExecutionException ex) {
                    Throwable cause = ex.getCause();
                    if (cause instanceof IOException) {
                        throw (IOException) cause;
                    }
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    if (cause instanceof Error) {
                        throw (Error) cause;
                    }
                    throw new AuthenticationTokenResolverException("Failed to retrieve response body!", cause);
                }

                // HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                String responsePayload = response.body();
                TokenMap responseMap = objectMapper.readValue(responsePayload, TokenMap.class);
                String requestedToken = responseMap.get("access_token");
                if (requestedToken == null || requestedToken.isBlank()) {
                    LOGGER.debug("HttpClient's token response did not contain an access token");
                    continue;
                }

                storeAuthToken(requestedToken);
                return requestedToken;
            }
            catch (HttpTimeoutException ex) {
                LOGGER.error("Timed out retrieving response body!", ex);
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new AuthenticationTokenResolverException("Interrupted while retrieving response body!", ex);
            }
            catch (IOException ex) {
                LOGGER.error("Failed to retrieve response body!", ex);
            }
        }

        throw new AuthenticationTokenResolverException("Failed to retrieve response body!");
    }

    @SuppressWarnings({"PMD.ExceptionAsFlowControl", "PMD.AvoidInstantiatingObjectsInLoops"})
    private String requestAuthToken(CloseableHttpClient client) {
        String requestPayload = StringSubstitutor.replace(
            authTokenRequestPayload,
            Map.of(
                "username", username,
                "password", password
            )
        );

        LOGGER.info("Requesting auth token via Apache CloseableHttpClient");
        for (int retryCounter = 0; retryCounter < MAX_RETRIES; retryCounter++) {
            if (retryCounter > 0) {
                waitBeforeRetry();
                LOGGER.debug("Retrying {}/{} on Apache CloseableHttpClient", retryCounter, MAX_RETRIES);
            }

            try {
                HttpPost request = new HttpPost(authServiceUrl);
                request.setEntity(new StringEntity(requestPayload, ContentType.APPLICATION_FORM_URLENCODED));
                try (CloseableHttpResponse response = client.execute(request)) {
                    String responsePayload = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                    TokenMap responseMap = objectMapper.readValue(responsePayload, TokenMap.class);
                    String requestedToken = responseMap.get("access_token");
                    if (requestedToken == null || requestedToken.isBlank()) {
                        LOGGER.debug("Apache CloseableHttpClient token response did not contain an access token");
                        continue;
                    }

                    storeAuthToken(requestedToken);
                    return requestedToken;
                }
            }
            catch (IOException | ParseException ex) {
                LOGGER.error("Failed to retrieve response body!", ex);
            }
        }

        throw new AuthenticationTokenResolverException("Failed to retrieve response body!");
    }

    @Override
    public synchronized String refreshToken(HttpClient client, String rejectedToken) {
        LOGGER.info("Refreshing auth token via HttpClient");
        try (
            FileChannel channel = FileChannel.open(
                persistentAuthTokenLockPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE
            );
            FileLock ignoredLock = channel.lock()
        ) {
            String sharedToken = loadAuthTokenFromFile();
            if (sharedToken != null && !sharedToken.equals(rejectedToken)) {
                LOGGER.info("Using auth token from HttpClient refreshed by another worker");
                token = sharedToken;
                return sharedToken;
            }

            LOGGER.info("Re-initializing token via HttpClient");
            String newToken = requestAuthToken(client);
            token = newToken;
            return newToken;
        }
        catch (IOException ex) {
            throw new AuthenticationTokenResolverException("Failed to acquire token refresh lock!", ex);
        }
    }

    @Override
    public synchronized String refreshToken(CloseableHttpClient client, String rejectedToken) {
        LOGGER.info("Refreshing auth token via Apache CloseableHttpClient");
        try (
            FileChannel channel = FileChannel.open(
                persistentAuthTokenLockPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE
            );
            FileLock ignoredLock = channel.lock()
        ) {
            String sharedToken = loadAuthTokenFromFile();
            if (sharedToken != null && !sharedToken.equals(rejectedToken)) {
                LOGGER.info("Using auth token from Apache CloseableHttpClient refreshed by another worker");
                token = sharedToken;
                return sharedToken;
            }

            LOGGER.info("Re-initializing token via Apache CloseableHttpClient");
            String newToken = requestAuthToken(client);
            token = newToken;
            return newToken;
        }
        catch (IOException ex) {
            throw new AuthenticationTokenResolverException("Failed to acquire token refresh lock!", ex);
        }
    }

    @SuppressWarnings("IllegalType")
    private static class TokenMap extends HashMap<String, String> {
        private static final long serialVersionUID = 6720739577283302699L;
    }

    private void waitBeforeRetry() {
        try {
            Thread.sleep(RETRY_DELAY);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
