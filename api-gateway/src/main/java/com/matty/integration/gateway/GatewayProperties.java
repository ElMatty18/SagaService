package com.matty.integration.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * @param apiKeys  API key => plan del cliente. (En producción: un servicio de identidad o
 *                 un store de keys hasheadas; acá se configura estático para la demo.)
 * @param plans    plan => cuota del token bucket.
 */
@ConfigurationProperties("gateway")
public record GatewayProperties(Routes routes, Map<String, Plan> apiKeys, Map<Plan, Quota> plans) {

    public enum Plan { FREE, PREMIUM }

    /**
     * Token bucket: el bucket tiene capacidad burstCapacity y se rellena a replenishRate tokens/seg.
     * Permite ráfagas cortas (burst) manteniendo un promedio sostenido (replenish).
     */
    public record Quota(int replenishRate, int burstCapacity) {
    }

    public record Routes(String orderServiceUrl, String catalogServiceUrl) {
    }
}
