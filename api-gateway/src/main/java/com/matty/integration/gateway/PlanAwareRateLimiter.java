package com.matty.integration.gateway;

import com.matty.integration.gateway.GatewayProperties.Plan;
import org.springframework.cloud.gateway.filter.ratelimit.RateLimiter;
import org.springframework.cloud.gateway.filter.ratelimit.RedisRateLimiter;
import org.springframework.context.ApplicationContext;
import reactor.core.publisher.Mono;

import java.util.EnumMap;
import java.util.Map;

/**
 * RATE LIMIT POR PLAN, DISTRIBUIDO.
 *
 * RedisRateLimiter de Spring Cloud Gateway implementa un token bucket con un script Lua
 * que corre ATÓMICAMENTE en Redis: aunque haya N réplicas del gateway, el límite es
 * global por cliente (un contador en memoria por réplica permitiría N veces la cuota).
 *
 * RedisRateLimiter tiene una sola cuota por ruta; para tener cuotas distintas por plan
 * se crea un limiter por plan y se elige según el prefijo de la clave ("PREMIUM:abc123").
 *
 * Fail-open: si Redis no responde, RedisRateLimiter DEJA PASAR el request (y lo loguea).
 * Es una decisión consciente: preferimos perder el rate limit unos segundos a tirar
 * toda la API porque se cayó una dependencia secundaria.
 *
 * Respuestas: headers X-RateLimit-Remaining / -Replenish-Rate / -Burst-Capacity, y 429 al exceder.
 */
public class PlanAwareRateLimiter implements RateLimiter<Object> {

    private final Map<Plan, RedisRateLimiter> limiters = new EnumMap<>(Plan.class);

    public PlanAwareRateLimiter(GatewayProperties props, ApplicationContext context) {
        props.plans().forEach((plan, quota) -> {
            RedisRateLimiter limiter = new RedisRateLimiter(quota.replenishRate(), quota.burstCapacity(), 1);
            // Inicializa el limiter con el ReactiveStringRedisTemplate y el script Lua del contexto.
            limiter.setApplicationContext(context);
            limiters.put(plan, limiter);
        });
    }

    @Override
    public Mono<Response> isAllowed(String routeId, String id) {
        Plan plan = Plan.valueOf(id.substring(0, id.indexOf(':')));
        RedisRateLimiter limiter = limiters.get(plan);
        if (limiter == null) {
            return Mono.error(new IllegalStateException("Plan sin cuota configurada: " + plan));
        }
        // routeId en la clave: la cuota es por cliente Y por ruta.
        return limiter.isAllowed(routeId, id);
    }

    @Override
    public Map<String, Object> getConfig() {
        return Map.of();
    }

    @Override
    public Class<Object> getConfigClass() {
        return Object.class;
    }

    @Override
    public Object newConfig() {
        return new Object();
    }
}
