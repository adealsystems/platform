package org.adealsystems.platform.http;

public class AuthenticationTokenResolverException extends RuntimeException {
    private static final long serialVersionUID = 7041732920302232643L;

    public AuthenticationTokenResolverException(String message) {
        super(message);
    }

    public AuthenticationTokenResolverException(String message, Throwable cause) {
        super(message, cause);
    }
}
