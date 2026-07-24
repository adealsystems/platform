package org.adealsystems.platform.http;

import java.net.http.HttpClient;

public interface AuthenticationTokenResolver {
    String resolveToken(HttpClient client);
    String refreshToken(HttpClient client, String rejectedToken);
}
