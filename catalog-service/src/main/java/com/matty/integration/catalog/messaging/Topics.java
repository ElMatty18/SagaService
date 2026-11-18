package com.matty.integration.catalog.messaging;

/**
 * Nombres de topics: <dominio-dueño>.<evento>.v<versión>.
 * El sufijo de versión permite publicar un v2 incompatible en paralelo mientras
 * los consumidores migran, sin romper a nadie.
 */
public final class Topics {

    public static final String ORDER_CREATED = "orders.order-created.v1";
    public static final String STOCK_RESERVED = "catalog.stock-reserved.v1";
    public static final String STOCK_REJECTED = "catalog.stock-rejected.v1";

    private Topics() {
    }
}
