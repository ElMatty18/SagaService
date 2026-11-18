package com.matty.integration.catalog.domain;

import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Inyección de fallas a nivel de aplicación, complementaria a Toxiproxy.
 * Toxiproxy degrada la RED (afecta a todas las rutas); esto permite degradar
 * un endpoint puntual, p. ej. sólo "discount" lento para ver cómo order-service
 * degrada ese dato opcional sin afectar el resto del pedido.
 *
 * Se configura en caliente vía PUT /admin/faults (ver FaultAdminController).
 * NO es algo que existiría en producción.
 */
@Component
public class FaultSimulator {

    private volatile int discountLatencyMs = 0;
    private volatile double productErrorRate = 0.0;

    public void maybeDelay(String operation) {
        if ("discount".equals(operation) && discountLatencyMs > 0) {
            try {
                Thread.sleep(discountLatencyMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void maybeFail(String operation) {
        if ("product".equals(operation) && ThreadLocalRandom.current().nextDouble() < productErrorRate) {
            throw new SimulatedFailureException("Falla simulada en " + operation);
        }
    }

    public void configure(int discountLatencyMs, double productErrorRate) {
        this.discountLatencyMs = Math.max(0, discountLatencyMs);
        this.productErrorRate = Math.min(1.0, Math.max(0.0, productErrorRate));
    }

    public int getDiscountLatencyMs() { return discountLatencyMs; }
    public double getProductErrorRate() { return productErrorRate; }

    public static class SimulatedFailureException extends RuntimeException {
        public SimulatedFailureException(String message) {
            super(message);
        }
    }
}
