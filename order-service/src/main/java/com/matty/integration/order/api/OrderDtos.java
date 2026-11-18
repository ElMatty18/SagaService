package com.matty.integration.order.api;

import com.matty.integration.order.domain.Order;
import com.matty.integration.order.domain.OrderStatus;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public final class OrderDtos {

    private OrderDtos() {
    }

    /** Límites también en el payload: protegen al catálogo de un fan-out gigante (N líneas => 3N llamadas). */
    public record CreateOrderRequest(
            @NotBlank @Size(max = 64) String customerId,
            @NotEmpty @Size(max = 20) List<@Valid OrderLineRequest> items) {
    }

    public record OrderLineRequest(
            @NotBlank @Size(max = 32) String sku,
            @Min(1) @Max(100) int quantity) {
    }

    public record OrderView(UUID id, String customerId, OrderStatus status, BigDecimal total, String currency,
                            boolean pricingDegraded, String rejectionReason, List<LineView> lines,
                            Instant createdAt, Instant updatedAt) {

        public static OrderView from(Order order) {
            return new OrderView(order.getId(), order.getCustomerId(), order.getStatus(), order.getTotal(),
                    order.getCurrency(), order.isPricingDegraded(), order.getRejectionReason(),
                    order.getLines().stream().map(l -> new LineView(l.getSku(), l.getQuantity(), l.getUnitPrice(),
                            l.getDiscountPercentage(), l.getLineTotal())).toList(),
                    order.getCreatedAt(), order.getUpdatedAt());
        }
    }

    public record LineView(String sku, int quantity, BigDecimal unitPrice, BigDecimal discountPercentage, BigDecimal lineTotal) {
    }
}
