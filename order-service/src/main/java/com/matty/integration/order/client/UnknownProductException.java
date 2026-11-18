package com.matty.integration.order.client;

/** 404 del catálogo: es una respuesta de NEGOCIO válida. No se reintenta ni abre el circuito. */
public class UnknownProductException extends RuntimeException {

    private final String sku;

    public UnknownProductException(String sku) {
        super("El producto " + sku + " no existe en el catálogo");
        this.sku = sku;
    }

    public String getSku() {
        return sku;
    }
}
