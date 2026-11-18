package com.matty.integration.gateway;

import com.matty.integration.gateway.GatewayProperties.Plan;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Autenticación por API key en el EDGE.
 *
 * - Sin key o key inválida => 401 antes de consumir recursos de rate limit o de backend.
 * - Resuelve el PLAN del cliente y lo deja como atributo del exchange para el rate limiter.
 * - Quita X-API-Key antes de reenviar: los servicios internos no necesitan (ni deben ver)
 *   la credencial; reciben X-Client-Id (un hash) y X-Client-Plan.
 */
@Component
public class ApiKeyAuthFilter implements GlobalFilter, Ordered {

    public static final String HEADER = "X-API-Key";
    /** Atributo con "<PLAN>:<clientId>", usado como clave de rate limit. */
    public static final String RATE_LIMIT_KEY_ATTR = "gateway.rateLimitKey";

    private final GatewayProperties props;

    public ApiKeyAuthFilter(GatewayProperties props) {
        this.props = props;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        if (!path.startsWith("/api/")) {
            return chain.filter(exchange); // actuator, fallback
        }

        String apiKey = exchange.getRequest().getHeaders().getFirst(HEADER);
        Plan plan = apiKey == null ? null : props.apiKeys().get(apiKey);
        if (plan == null) {
            return unauthorized(exchange);
        }

        String clientId = hash(apiKey);
        exchange.getAttributes().put(RATE_LIMIT_KEY_ATTR, plan + ":" + clientId);

        ServerHttpRequest request = exchange.getRequest().mutate()
                .headers(h -> {
                    h.remove(HEADER);
                    h.set("X-Client-Id", clientId);
                    h.set("X-Client-Plan", plan.name());
                })
                .build();
        return chain.filter(exchange.mutate().request(request).build());
    }

    private Mono<Void> unauthorized(ServerWebExchange exchange) {
        var response = exchange.getResponse();
        response.setStatusCode(HttpStatus.UNAUTHORIZED);
        response.getHeaders().setContentType(MediaType.APPLICATION_PROBLEM_JSON);
        response.getHeaders().set("WWW-Authenticate", "ApiKey header=\"" + HEADER + "\"");
        byte[] body = """
                {"type":"urn:problem:gateway:unauthorized","title":"Unauthorized","status":401,\
                "detail":"Falta el header X-API-Key o no es válido"}""".getBytes(StandardCharsets.UTF_8);
        DataBuffer buffer = response.bufferFactory().wrap(body);
        return response.writeWith(Mono.just(buffer));
    }

    private static String hash(String apiKey) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(apiKey.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Corre después del correlation id y antes de los filtros de ruta (rate limiter). */
    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 10;
    }
}
