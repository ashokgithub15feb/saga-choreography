package com.javatechie.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * One place that turns exceptions into HTTP responses (RFC 9457 ProblemDetail JSON).
 * ResponseEntityExceptionHandler already covers Spring MVC's own exceptions with a 4xx ProblemDetail, e.g.
 * missing Idempotency-Key header, @NotBlank / @Size violations, unreadable JSON body.
 */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(BookingNotFoundException.class)
    public ProblemDetail handleBookingNotFound(BookingNotFoundException ex) {
        return problem(HttpStatus.NOT_FOUND, "Booking not found", ex.getMessage());
    }

    // Same Idempotency-Key sent again with a different request body -> client bug
    @ExceptionHandler(IdempotencyKeyReuseException.class)
    public ProblemDetail handleIdempotencyKeyReuse(IdempotencyKeyReuseException ex) {
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, "Idempotency-Key reused", ex.getMessage());
    }

    /*
     * A unique constraint was hit. For bookings that means two requests with the same Idempotency-Key arrived
     * at the same moment and the other one inserted first. 409 tells the client to retry with the same key;
     * the retry then gets the stored booking.
     */
    @ExceptionHandler(DataIntegrityViolationException.class)
    public ProblemDetail handleDataIntegrityViolation(DataIntegrityViolationException ex) {
        log.warn("Data integrity violation: {}", ex.getMostSpecificCause().getMessage());
        return problem(HttpStatus.CONFLICT, "Conflicting request",
                "The request conflicts with existing data, most likely another request with the same "
                        + "Idempotency-Key is still in progress. Retry with the same key to get its result.");
    }

    // Anything unexpected: log the details, don't leak them to the client
    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception ex) {
        log.error("Unhandled exception", ex);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Internal error", "Unexpected error, please retry later");
    }

    private static ProblemDetail problem(HttpStatus status, String title, String detail) {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(status, detail);
        problemDetail.setTitle(title);
        return problemDetail;
    }
}
