package com.matty.integration.order.domain;

import com.matty.integration.order.quote.Quote;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    private UUID id;
    @Column(name = "customer_id")
    private String customerId;
    @Enumerated(EnumType.STRING)
    private OrderStatus status;
    private BigDecimal total;
    private String currency;
    @Column(name = "pricing_degraded")
    private boolean pricingDegraded;
    @Column(name = "rejection_reason")
    private String rejectionReason;
    @Column(name = "created_at")
    private Instant createdAt;
    @Column(name = "updated_at")
    private Instant updatedAt;
    /** Optimistic locking: dos eventos concurrentes sobre el mismo pedido no se pisan. */
    @Version
    private long version;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<OrderLine> lines = new ArrayList<>();

    protected Order() {
    }

    public static Order place(UUID id, String customerId, Quote quote) {
        Order order = new Order();
        order.id = id;
        order.customerId = customerId;
        order.status = OrderStatus.PENDING;
        order.total = quote.total();
        order.currency = quote.currency();
        order.pricingDegraded = quote.degraded();
        order.createdAt = Instant.now();
        order.updatedAt = order.createdAt;
        quote.lines().forEach(l -> order.lines.add(new OrderLine(order, l.sku(), l.quantity(), l.unitPrice(),
                l.discountPercentage(), l.lineTotal())));
        return order;
    }

    /** @return false si el pedido ya no estaba PENDING (evento repetido o tardío). */
    public boolean confirm() {
        if (status != OrderStatus.PENDING) {
            return false;
        }
        status = OrderStatus.CONFIRMED;
        updatedAt = Instant.now();
        return true;
    }

    public boolean reject(String reason) {
        if (status != OrderStatus.PENDING) {
            return false;
        }
        status = OrderStatus.REJECTED;
        rejectionReason = reason;
        updatedAt = Instant.now();
        return true;
    }

    public UUID getId() { return id; }
    public String getCustomerId() { return customerId; }
    public OrderStatus getStatus() { return status; }
    public BigDecimal getTotal() { return total; }
    public String getCurrency() { return currency; }
    public boolean isPricingDegraded() { return pricingDegraded; }
    public String getRejectionReason() { return rejectionReason; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public List<OrderLine> getLines() { return lines; }
}
