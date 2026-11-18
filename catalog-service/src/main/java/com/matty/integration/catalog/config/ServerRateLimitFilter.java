package com.matty.integration.catalog.config;

import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * LOAD SHEDDING del provider.
 *
 * Es preferible rechazar rápido con 429 a aceptar todo y degradar la latencia
 * de TODOS los requests (que es lo que provoca timeouts en cascada en los consumidores).
 * El header Retry-After le dice al cliente cuándo vale la pena reintentar.
 *
 * Sólo aplica a /api/** (la API de negocio); actuator y admin quedan fuera para que
 * health checks y métricas sigan funcionando bajo carga.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class ServerRateLimitFilter extends OncePerRequestFilter {

    private final RateLimiter rateLimiter;

    public ServerRateLimitFilter(RateLimiterRegistry registry) {
        this.rateLimiter = registry.rateLimiter("catalog-api");
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith("/api/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if (!rateLimiter.acquirePermission()) {
            response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
            response.setHeader("Retry-After", "1");
            response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
            response.getWriter().write("""
                    {"type":"urn:problem:catalog:rate-limited","title":"Too Many Requests","status":429,\
                    "detail":"catalog-service está limitando la carga, reintentar más tarde"}""");
            return;
        }
        chain.doFilter(request, response);
    }
}
