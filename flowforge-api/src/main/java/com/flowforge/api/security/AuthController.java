package com.flowforge.api.security;

import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Dev token issuer. In production this endpoint is replaced by an external identity provider. */
@RestController
@RequestMapping("/api/v1/auth")
public class AuthController {

    public record TokenRequest(String username, String password) {}

    public record TokenResponse(String accessToken, String tokenType, long expiresIn, String scope) {}

    private final Map<String, SecurityProperties.User> users;
    private final SecurityProperties props;
    private final PasswordEncoder passwords;
    private final JwtEncoder jwtEncoder;
    private final Clock clock;

    public AuthController(SecurityProperties props, PasswordEncoder passwords, JwtEncoder jwtEncoder, Clock clock) {
        this.props = props;
        this.passwords = passwords;
        this.jwtEncoder = jwtEncoder;
        this.clock = clock;
        this.users = props.users().stream()
                .collect(Collectors.toUnmodifiableMap(SecurityProperties.User::username, Function.identity()));
    }

    @PostMapping("/token")
    public TokenResponse token(@RequestBody TokenRequest request) {
        SecurityProperties.User user = request.username() == null ? null : users.get(request.username());
        if (user == null || request.password() == null || !passwords.matches(request.password(), user.password())) {
            throw new BadCredentialsException("invalid username or password");
        }
        Instant now = clock.instant();
        String scope = String.join(" ", user.scopes());
        JwtClaimsSet claims = JwtClaimsSet.builder()
                .issuer(SecurityConfig.ISSUER)
                .subject(user.username())
                .issuedAt(now)
                .expiresAt(now.plus(props.tokenTtl()))
                .claim("scope", scope)
                .build();
        String token = jwtEncoder.encode(JwtEncoderParameters.from(JwsHeader.with(MacAlgorithm.HS256).build(), claims))
                .getTokenValue();
        return new TokenResponse(token, "Bearer", props.tokenTtl().toSeconds(), scope);
    }
}
