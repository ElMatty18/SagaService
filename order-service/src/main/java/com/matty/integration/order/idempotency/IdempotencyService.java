package com.matty.integration.order.idempotency;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;

/**
 * IDEMPOTENCY-KEY en el POST (estilo Stripe / draft IETF "Idempotency-Key header").
 *
 * POST no es idempotente por naturaleza: si el cliente no recibe la respuesta (timeout,
 * corte de red) y reintenta, sin esto se crearían dos pedidos. Con la key:
 *   - misma key + mismo body   => se devuelve el pedido ya creado (sin efectos nuevos)
 *   - misma key + otro body    => 422 (el cliente reutilizó la key por error)
 *   - requests concurrentes    => la PK de idempotency_keys deja pasar a una sola
 */
@Service
public class IdempotencyService {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public IdempotencyService(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    public Optional<UUID> lookup(String key, String requestHash) {
        return jdbc.query("SELECT request_hash, order_id FROM idempotency_keys WHERE idempotency_key = ?",
                        (rs, i) -> new Stored(rs.getString("request_hash"), rs.getObject("order_id", UUID.class)), key)
                .stream().findFirst()
                .map(stored -> {
                    if (!stored.requestHash().equals(requestHash)) {
                        throw new IdempotencyKeyReusedException(key);
                    }
                    return stored.orderId();
                });
    }

    /** Lanza DuplicateKeyException si otra transacción ya registró la key. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void register(String key, String requestHash, UUID orderId) {
        jdbc.update("INSERT INTO idempotency_keys (idempotency_key, request_hash, order_id) VALUES (?, ?, ?)",
                key, requestHash, orderId);
    }

    public String fingerprint(Object request) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(mapper.writeValueAsString(request).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (JsonProcessingException | NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private record Stored(String requestHash, UUID orderId) {
    }

    public static class IdempotencyKeyReusedException extends RuntimeException {
        public IdempotencyKeyReusedException(String key) {
            super("La Idempotency-Key '" + key + "' ya fue usada con un request distinto");
        }
    }
}
