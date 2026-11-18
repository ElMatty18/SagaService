package com.matty.integration.catalog.domain;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

import java.util.Collection;
import java.util.List;

public interface ProductRepository extends JpaRepository<Product, String> {

    /**
     * SELECT ... FOR UPDATE ordenado por SKU.
     * Bloquear siempre en el mismo orden evita deadlocks cuando dos pedidos
     * concurrentes reservan los mismos productos en distinto orden.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select p from Product p where p.sku in :skus order by p.sku")
    List<Product> findAllForUpdate(Collection<String> skus);
}
