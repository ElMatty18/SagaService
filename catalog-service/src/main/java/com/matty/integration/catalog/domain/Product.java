package com.matty.integration.catalog.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.math.BigDecimal;

@Entity
@Table(name = "products")
public class Product {

    @Id
    private String sku;
    private String name;
    private String category;
    @Column(name = "unit_price")
    private BigDecimal unitPrice;
    private String currency;
    private int stock;
    @Version
    private long version;

    protected Product() {
    }

    public Product(String sku, String name, String category, BigDecimal unitPrice, String currency, int stock) {
        this.sku = sku;
        this.name = name;
        this.category = category;
        this.unitPrice = unitPrice;
        this.currency = currency;
        this.stock = stock;
    }

    public boolean hasStock(int quantity) {
        return stock >= quantity;
    }

    public void decreaseStock(int quantity) {
        if (!hasStock(quantity)) {
            throw new IllegalStateException("Stock insuficiente para " + sku);
        }
        this.stock -= quantity;
    }

    public String getSku() { return sku; }
    public String getName() { return name; }
    public String getCategory() { return category; }
    public BigDecimal getUnitPrice() { return unitPrice; }
    public String getCurrency() { return currency; }
    public int getStock() { return stock; }
}
