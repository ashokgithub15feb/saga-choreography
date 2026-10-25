package com.javatechie.exception;

// Same Idempotency-Key sent again with a different request body -> 422 (GlobalExceptionHandler)
public class IdempotencyKeyReuseException extends RuntimeException {

    public IdempotencyKeyReuseException(String idempotencyKey) {
        super("Idempotency-Key " + idempotencyKey + " was already used with a different request");
    }
}
