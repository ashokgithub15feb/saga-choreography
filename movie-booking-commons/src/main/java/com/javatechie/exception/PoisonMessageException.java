package com.javatechie.exception;

/**
 * Thrown when a message can never be processed successfully (bad data, a bug).
 * The Kafka error handler treats it as NOT retryable and sends the record straight to the DLT.
 */
public class PoisonMessageException extends RuntimeException {

    public PoisonMessageException(String message) {
        super(message);
    }
}
