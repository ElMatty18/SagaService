package com.matty.integration.catalog.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;

@Entity
@Table(name = "discounts")
public class Discount {

    @Id
    private Long id;
    private String category;
    @Column(name = "min_quantity")
    private int minQuantity;
    private BigDecimal percentage;

    protected Discount() {
    }

    public String getCategory() { return category; }
    public int getMinQuantity() { return minQuantity; }
    public BigDecimal getPercentage() { return percentage; }
}
