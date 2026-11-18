package com.matty.integration.order.quote;

import com.matty.integration.order.client.CatalogDtos.PriceInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fallback "último valor conocido" para precios.
 *
 * Cada respuesta exitosa del catálogo se guarda; si después el catálogo no responde
 * (circuito abierto, timeout), cotizamos con el precio cacheado siempre que no supere el TTL.
 * Es una decisión de negocio consciente: preferimos vender con un precio de hace
 * unos minutos a no vender. El pedido queda marcado como "pricingDegraded".
 *
 * (En producción con varias réplicas sería un cache compartido, p. ej. Redis.)
 */
@Component
public class LastKnownPriceCache {

    private final Map<String, Entry> entries = new ConcurrentHashMap<>();
    private final QuoteProperties props;
    private final Clock clock;

    @Autowired
    public LastKnownPriceCache(QuoteProperties props) {
        this(props, Clock.systemUTC());
    }

    LastKnownPriceCache(QuoteProperties props, Clock clock) {
        this.props = props;
        this.clock = clock;
    }

    public void put(PriceInfo price) {
        entries.put(price.sku(), new Entry(price, clock.instant()));
    }

    public Optional<PriceInfo> get(String sku) {
        Entry entry = entries.get(sku);
        if (entry == null || entry.storedAt().plus(props.priceCacheTtl()).isBefore(clock.instant())) {
            return Optional.empty();
        }
        return Optional.of(entry.price());
    }

    private record Entry(PriceInfo price, Instant storedAt) {
    }
}
