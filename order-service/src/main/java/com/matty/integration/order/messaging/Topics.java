package com.matty.integration.order.messaging;

/** Ver convención en catalog-service: <dominio-dueño>.<evento>.v<versión>. */
public final class Topics {

    public static final String ORDER_CREATED = "orders.order-created.v1";
    public static final String STOCK_RESERVED = "catalog.stock-reserved.v1";
    public static final String STOCK_REJECTED = "catalog.stock-rejected.v1";
    /** Sufijo de los dead letter topics de los consumidores de order-service. */
    public static final String DLT_SUFFIX = "-dlt";

    private Topics() {
    }
}
