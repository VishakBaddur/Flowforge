package com.flowforge.api.security;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

/**
 * jwtSecret: HS256 key shared by all API replicas (>= 32 bytes). In Kubernetes it comes from a Secret.
 * users: dev-only identity store for the token endpoint; production would use an external IdP.
 */
@ConfigurationProperties("flowforge.security")
public record SecurityProperties(String jwtSecret, Duration tokenTtl, List<User> users) {

    public record User(String username, String password, List<String> scopes) {}
}
