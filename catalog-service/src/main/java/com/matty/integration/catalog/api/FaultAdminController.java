package com.matty.integration.catalog.api;

import com.matty.integration.catalog.domain.FaultSimulator;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Endpoint SÓLO para la demo: permite inyectar fallas de aplicación en caliente. */
@RestController
@RequestMapping("/admin/faults")
public class FaultAdminController {

    private final FaultSimulator faults;

    public FaultAdminController(FaultSimulator faults) {
        this.faults = faults;
    }

    public record FaultConfig(int discountLatencyMs, double productErrorRate) {
    }

    @GetMapping
    public FaultConfig get() {
        return new FaultConfig(faults.getDiscountLatencyMs(), faults.getProductErrorRate());
    }

    @PutMapping
    public FaultConfig update(@RequestBody FaultConfig config) {
        faults.configure(config.discountLatencyMs(), config.productErrorRate());
        return get();
    }
}
