package com.matty.integration.order.domain;

/**
 * Máquina de estados del pedido dentro del saga:
 *   PENDING --StockReserved--> CONFIRMED
 *   PENDING --StockRejected--> REJECTED
 * Las transiciones desde estados finales se ignoran (eventos duplicados o fuera de orden).
 */
public enum OrderStatus {
    PENDING, CONFIRMED, REJECTED
}
