package com.javatechie.simulator;

import com.javatechie.exception.PaymentServiceException;
import com.javatechie.exception.PoisonMessageException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Demo only: makes payment-service (step C of the saga) fail on purpose so the retry / DLT path can be observed.
 * Mode is switched at runtime through FailureSimulatorController (dev profile). Default NONE = normal behaviour.
 */
@Component
@Slf4j
public class FailureSimulator {

    public enum Mode {
        NONE,       // no failure
        FLAKY,      // transient: first 2 attempts fail, 3rd succeeds -> recovered by retry, no DLT
        TRANSIENT,  // transient that keeps failing: 1 try + 3 retries (1s, 2s, 4s) -> DLT
        POISON      // bad data: no retry -> DLT immediately
    }

    private static final int FLAKY_FAILURES = 2;

    private final AtomicReference<Mode> mode = new AtomicReference<>(Mode.NONE);
    private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

    public Mode getMode() {
        return mode.get();
    }

    public void setMode(Mode newMode) {
        mode.set(newMode);
        attempts.clear();
        log.warn("FailureSimulator:: mode set to {}", newMode);
    }

    public void maybeFail(String bookingId) {
        switch (mode.get()) {
            case NONE -> { }
            case FLAKY -> {
                int attempt = attempts.computeIfAbsent(bookingId, k -> new AtomicInteger()).incrementAndGet();
                if (attempt <= FLAKY_FAILURES) {
                    throw new PaymentServiceException("Simulated payment gateway timeout for bookingId "
                            + bookingId + " (attempt " + attempt + ")");
                }
                attempts.remove(bookingId);
                log.info("FailureSimulator:: bookingId {} succeeded on attempt {}", bookingId, attempt);
            }
            case TRANSIENT -> throw new PaymentServiceException("Simulated payment gateway timeout for bookingId " + bookingId);
            case POISON -> throw new PoisonMessageException("Simulated bad payment data for bookingId " + bookingId);
        }
    }
}
