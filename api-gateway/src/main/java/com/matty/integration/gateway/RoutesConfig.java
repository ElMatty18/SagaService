package com.matty.integration.gateway;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import org.springframework.cloud.circuitbreaker.resilience4j.ReactiveResilience4JCircuitBreakerFactory;
import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JConfigBuilder;
import org.springframework.cloud.client.circuitbreaker.Customizer;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpMethod;
import org.springframework.util.unit.DataSize;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Rutas del gateway, definidas en Java para que cada filtro quede documentado.
 * Orden de filtros en la ruta de pedidos:
 *
 *   ApiKeyAuth (global) → RequestRateLimiter (Redis) → RequestSize → CircuitBreaker → order-service
 *
 * catalog-service es INTERNO: sólo se expone el listado de productos (GET) para la demo.
 */
@Configuration
public class RoutesConfig {

    @Bean
    RouteLocator routes(RouteLocatorBuilder builder, GatewayProperties props,
                        PlanAwareRateLimiter rateLimiter, KeyResolver clientKeyResolver) {
        return builder.routes()
                .route("orders", r -> r.path("/api/v1/orders", "/api/v1/orders/**")
                        .filters(f -> f
                                .requestRateLimiter(c -> c.setRateLimiter(rateLimiter).setKeyResolver(clientKeyResolver))
                                // Rechaza payloads gigantes en el borde (413), antes de llegar al servicio.
                                .setRequestSize(DataSize.ofKilobytes(64))
                                // Circuit breaker de BORDE: si order-service está caído/colgado,
                                // respondemos 503 inmediato en vez de acumular conexiones.
                                .circuitBreaker(c -> c.setName("order-service").setFallbackUri("forward:/fallback/orders"))
                                // Reintento SÓLO para GET (idempotente). Nunca POST: podría duplicar pedidos.
                                .retry(c -> c.setRetries(2).setMethods(HttpMethod.GET)
                                        .setBackoff(Duration.ofMillis(100), Duration.ofMillis(1000), 2, true)))
                        .uri(props.routes().orderServiceUrl()))
                .route("products", r -> r.path("/api/v1/products").and().method(HttpMethod.GET)
                        .filters(f -> f
                                .requestRateLimiter(c -> c.setRateLimiter(rateLimiter).setKeyResolver(clientKeyResolver))
                                .circuitBreaker(c -> c.setName("catalog-service").setFallbackUri("forward:/fallback/products")))
                        .uri(props.routes().catalogServiceUrl()))
                .build();
    }

    @Bean
    @Primary
    PlanAwareRateLimiter planAwareRateLimiter(GatewayProperties props, ApplicationContext context) {
        return new PlanAwareRateLimiter(props, context);
    }

    /** Clave de rate limit = "<PLAN>:<clientId>", calculada por ApiKeyAuthFilter. */
    @Bean
    KeyResolver clientKeyResolver() {
        return exchange -> Mono.justOrEmpty(exchange.<String>getAttribute(ApiKeyAuthFilter.RATE_LIMIT_KEY_ATTR));
    }

    /**
     * Política del circuit breaker de borde. El TimeLimiter (4s) debe ser MAYOR que el
     * presupuesto interno de order-service (2.5s): el gateway sólo corta si order-service
     * no respeta su propio presupuesto (está colgado), no le gana la carrera.
     */
    @Bean
    Customizer<ReactiveResilience4JCircuitBreakerFactory> edgeCircuitBreakerCustomizer() {
        return factory -> factory.configureDefault(id -> new Resilience4JConfigBuilder(id)
                .circuitBreakerConfig(CircuitBreakerConfig.custom()
                        .slidingWindowSize(20)
                        .minimumNumberOfCalls(10)
                        .failureRateThreshold(50)
                        .waitDurationInOpenState(Duration.ofSeconds(15))
                        .permittedNumberOfCallsInHalfOpenState(3)
                        .build())
                .timeLimiterConfig(TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(4)).build())
                .build());
    }
}
