package com.matty.integration.order.client;

/** 429 del catálogo: es transitorio (se reintenta con backoff), pero lo distinguimos para métricas/logs. */
public class CatalogThrottledException extends CatalogUnavailableException {

    public CatalogThrottledException(String retryAfter) {
        super("catalog-service respondió 429 (Retry-After: " + retryAfter + ")");
    }
}
