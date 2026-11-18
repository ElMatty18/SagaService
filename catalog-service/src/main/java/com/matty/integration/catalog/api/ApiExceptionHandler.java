package com.matty.integration.catalog.api;

import com.matty.integration.catalog.domain.FaultSimulator.SimulatedFailureException;
import com.matty.integration.catalog.domain.ProductNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.net.URI;

/**
 * Errores con formato RFC 7807 (application/problem+json).
 * Un formato de error estable es parte del contrato: el consumidor decide si
 * reintenta o no según el status (4xx = no reintentar, 5xx/429 = reintentable).
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(ProductNotFoundException.class)
    ProblemDetail notFound(ProductNotFoundException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        problem.setType(URI.create("urn:problem:catalog:product-not-found"));
        problem.setTitle("Product not found");
        return problem;
    }

    @ExceptionHandler(SimulatedFailureException.class)
    ProblemDetail simulated(SimulatedFailureException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
        problem.setType(URI.create("urn:problem:catalog:simulated-failure"));
        return problem;
    }
}
