package com.javatechie.simulator;

import com.javatechie.request.BookingRequest;
import com.javatechie.response.BookingResponse;
import com.javatechie.response.SeatResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static com.javatechie.common.KafkaConfigProperties.BOOKING_REQUEST_DLT_TOPIC;

/**
 * Demo / load test: a "rush" of 10-15 people trying to book the same few seats at the same moment.
 * <p>
 * Every run:
 * 1. asks seat-inventory-service (over HTTP, its database is not ours) for AVAILABLE seats
 * 2. picks one random show and a few of its seats that everybody fights for
 * 3. fires all POST /bookSeat requests at once (random user, seat and amount, new Idempotency-Key each)
 * 4. waits for the saga to settle, reads every booking's status and checks that no seat was confirmed twice
 * <p>
 * Failure handling of the HTTP call (chaos makes some users fail on purpose):
 * - transient (5xx, 408, 409, 429, I/O error) -> retried up to max-retries times, backoff 1s, 2s, 4s
 * - permanent (other 4xx: 400, 404, 422)      -> no retry, a retry would fail the same way
 * - still failing after the retries, or permanent -> published to booking-request-dlt
 * All retries reuse the SAME Idempotency-Key: if a "failed" attempt actually created the booking,
 * the retry gets that booking back instead of creating a second one.
 * <p>
 * Off by default (each run uses up seats): start booking-service with booking.simulator.enabled=true.
 */
@Component
@ConditionalOnProperty(name = "booking.simulator.enabled", havingValue = "true")
@Slf4j
public class BookingLoadSimulator {

    private static final List<String> USERS = List.of("John", "Kushi", "Kashi", "Asha", "Ravi", "Meera", "Arjun",
            "Priya", "Vikram", "Neha", "Rahul", "Divya", "Kiran", "Anil", "Sita");

    /** What goes wrong for a simulated user. */
    enum Chaos {
        NONE,           // normal request
        FLAKY_503,      // 503 on the first 2 attempts, then OK  -> recovered by retry
        ALWAYS_503,     // 503 on every attempt                  -> retries exhausted -> DLT
        BAD_REQUEST     // real call without Idempotency-Key     -> 400, not retryable -> DLT at once
    }

    private final RestClient bookingClient;
    private final RestClient seatClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final int minUsers;
    private final int maxUsers;
    private final int contestedSeats;
    private final Duration settleTime;
    private final int maxRetries;
    private final Duration retryBackoff;
    private final boolean chaosEnabled;
    // one virtual thread per simulated user, so all requests really run in parallel
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public BookingLoadSimulator(RestClient.Builder restClientBuilder,
                                KafkaTemplate<String, Object> kafkaTemplate,
                                @Value("${booking.simulator.booking-url}") String bookingUrl,
                                @Value("${booking.simulator.seat-inventory-url}") String seatInventoryUrl,
                                @Value("${booking.simulator.min-users:10}") int minUsers,
                                @Value("${booking.simulator.max-users:15}") int maxUsers,
                                @Value("${booking.simulator.contested-seats:3}") int contestedSeats,
                                @Value("${booking.simulator.settle-time:PT45S}") Duration settleTime,
                                @Value("${booking.simulator.max-retries:3}") int maxRetries,
                                @Value("${booking.simulator.retry-backoff:PT1S}") Duration retryBackoff,
                                @Value("${booking.simulator.chaos:true}") boolean chaosEnabled) {
        this.bookingClient = restClientBuilder.clone().baseUrl(bookingUrl).build();
        this.seatClient = restClientBuilder.clone().baseUrl(seatInventoryUrl).build();
        this.kafkaTemplate = kafkaTemplate;
        this.minUsers = minUsers;
        this.maxUsers = maxUsers;
        this.contestedSeats = contestedSeats;
        this.settleTime = settleTime;
        this.maxRetries = maxRetries;
        this.retryBackoff = retryBackoff;
        this.chaosEnabled = chaosEnabled;
    }

    // hands the rush to a virtual thread so the shared scheduler thread (OutboxRelay, timeouts) is never blocked
    @Scheduled(initialDelayString = "${booking.simulator.initial-delay:PT20S}",
            fixedDelayString = "${booking.simulator.interval:PT5M}")
    public void triggerRush() {
        executor.submit(this::rush);
    }

    private void rush() {
        try {
            List<SeatResponse> available = seatClient.get()
                    .uri("/seat-inventory-service/seats?status=AVAILABLE")
                    .retrieve()
                    .body(new ParameterizedTypeReference<List<SeatResponse>>() {});
            if (available == null || available.isEmpty()) {
                log.warn("BookingLoadSimulator:: No AVAILABLE seats left, skipping this rush");
                return;
            }

            // one random show, then a few of its seats that everybody fights for
            ThreadLocalRandom random = ThreadLocalRandom.current();
            String showId = available.get(random.nextInt(available.size())).showId();
            List<String> pool = available.stream()
                    .filter(s -> s.showId().equals(showId))
                    .map(SeatResponse::seatNumber)
                    .collect(Collectors.toCollection(ArrayList::new));
            Collections.shuffle(pool);
            pool = pool.subList(0, Math.min(contestedSeats, pool.size()));

            int people = random.nextInt(minUsers, maxUsers + 1);
            log.info("BookingLoadSimulator:: RUSH - {} people booking {} seats {} at the same moment", people, showId, pool);

            // everybody waits at the gate, then all requests are released together
            CountDownLatch gate = new CountDownLatch(1);
            List<Future<Attempt>> futures = new ArrayList<>();
            for (int i = 0; i < people; i++) {
                String user = USERS.get(random.nextInt(USERS.size())) + "-" + (i + 1);
                String seat = pool.get(random.nextInt(pool.size()));
                long amount = random.nextInt(3, 26) * 100L; // 300..2500, so some payments (> 2000) fail
                Chaos chaos = pickChaos(random);
                futures.add(executor.submit(() -> {
                    gate.await();
                    return bookWithRetry(user, showId, seat, amount, chaos);
                }));
            }
            gate.countDown();

            List<Attempt> attempts = new ArrayList<>();
            for (Future<Attempt> future : futures) {
                attempts.add(future.get());
            }
            log.info("BookingLoadSimulator:: {} requests done, checking results in {}", attempts.size(), settleTime);

            Thread.sleep(settleTime.toMillis());
            report(showId, attempts);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("BookingLoadSimulator:: Rush failed: {}", e.getMessage(), e);
        }
    }

    // ~60% normal, ~15% flaky, ~15% always failing, ~10% bad request
    private Chaos pickChaos(ThreadLocalRandom random) {
        if (!chaosEnabled) {
            return Chaos.NONE;
        }
        int roll = random.nextInt(100);
        if (roll < 60) return Chaos.NONE;
        if (roll < 75) return Chaos.FLAKY_503;
        if (roll < 90) return Chaos.ALWAYS_503;
        return Chaos.BAD_REQUEST;
    }

    /*
     * 1 attempt + up to maxRetries retries with exponential backoff (1s, 2s, 4s).
     * Same Idempotency-Key on every attempt. Gives up -> booking-request-dlt.
     */
    private Attempt bookWithRetry(String user, String showId, String seat, long amount, Chaos chaos) throws InterruptedException {
        BookingRequest request = new BookingRequest(null, showId, List.of(seat), user, Instant.now(), amount);
        String idempotencyKey = UUID.randomUUID().toString();
        int maxAttempts = maxRetries + 1;
        long backoff = retryBackoff.toMillis();

        for (int attempt = 1; ; attempt++) {
            try {
                BookingResponse response = send(request, idempotencyKey, chaos, attempt);
                if (attempt > 1) {
                    log.info("BookingLoadSimulator:: {} succeeded on attempt {}/{}", user, attempt, maxAttempts);
                }
                return new Attempt(user, seat, amount, chaos, attempt, response.reservationId(), null, false);
            } catch (Exception e) {
                String error = describe(e);
                boolean retryable = isRetryable(e);
                if (retryable && attempt < maxAttempts) {
                    log.warn("BookingLoadSimulator:: {} attempt {}/{} failed: {} -> retry in {} ms",
                            user, attempt, maxAttempts, error, backoff);
                    Thread.sleep(backoff);
                    backoff *= 2;
                    continue;
                }
                String reason = retryable ? "retries exhausted after " + attempt + " attempts" : "not retryable";
                log.error("BookingLoadSimulator:: {} gave up ({}): {} -> {}", user, reason, error, BOOKING_REQUEST_DLT_TOPIC);
                boolean sent = sendToDlt(new FailedBookingRequest(user, showId, List.of(seat), amount, idempotencyKey,
                        attempt, error, reason, chaos.name(), Instant.now()));
                return new Attempt(user, seat, amount, chaos, attempt, null, error, sent);
            }
        }
    }

    private BookingResponse send(BookingRequest request, String idempotencyKey, Chaos chaos, int attempt) {
        // injected failures: the request never reaches the service, as if a proxy / load balancer returned 503
        if (chaos == Chaos.ALWAYS_503 || (chaos == Chaos.FLAKY_503 && attempt <= 2)) {
            throw HttpServerErrorException.create(HttpStatus.SERVICE_UNAVAILABLE, "Simulated 503 (" + chaos + ")",
                    HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8);
        }
        RestClient.RequestBodySpec post = bookingClient.post()
                .uri("/booking-service/bookSeat")
                .contentType(MediaType.APPLICATION_JSON);
        if (chaos != Chaos.BAD_REQUEST) {
            post = post.header("Idempotency-Key", idempotencyKey); // BAD_REQUEST omits it -> real 400 from the API
        }
        return post.body(request).retrieve().body(BookingResponse.class);
    }

    // 5xx, timeouts, rate limit, concurrent duplicate (409) and I/O errors may succeed later; other 4xx never will
    private static boolean isRetryable(Exception e) {
        if (e instanceof ResourceAccessException) {
            return true;
        }
        if (e instanceof RestClientResponseException http) {
            int status = http.getStatusCode().value();
            return status >= 500 || status == 408 || status == 409 || status == 429;
        }
        return false;
    }

    private static String describe(Exception e) {
        if (e instanceof RestClientResponseException http) {
            return http.getStatusCode().value() + " " + http.getStatusText();
        }
        return e.getClass().getSimpleName() + ": " + e.getMessage();
    }

    // template is transactional and we are outside a listener / DB transaction -> local Kafka transaction
    private boolean sendToDlt(FailedBookingRequest failed) {
        try {
            kafkaTemplate.executeInTransaction(ops -> ops.send(BOOKING_REQUEST_DLT_TOPIC, failed.idempotencyKey(), failed));
            return true;
        } catch (Exception e) {
            log.error("BookingLoadSimulator:: Could not publish {} to {}: {}", failed.userId(), BOOKING_REQUEST_DLT_TOPIC, e.getMessage());
            return false;
        }
    }

    private void report(String showId, List<Attempt> attempts) {
        StringBuilder table = new StringBuilder();
        Map<String, List<String>> confirmedBySeat = new TreeMap<>();
        Map<String, Integer> totals = new TreeMap<>();

        for (Attempt attempt : attempts) {
            String status = attempt.error() != null
                    ? (attempt.sentToDlt() ? "DLT" : "DLT_PUBLISH_FAILED")
                    : statusOf(attempt.reservationId());
            totals.merge(status, 1, Integer::sum);
            if ("CONFIRMED".equals(status)) {
                confirmedBySeat.computeIfAbsent(attempt.seat(), k -> new ArrayList<>()).add(attempt.user());
            }
            table.append(String.format("%n  %-10s seat %-4s amount %5d chaos %-11s attempts %d -> %-10s %s",
                    attempt.user(), attempt.seat(), attempt.amount(), attempt.chaos(), attempt.attempts(), status,
                    attempt.error() != null ? attempt.error() : attempt.reservationId()));
        }

        log.info("BookingLoadSimulator:: RESULT for {} {}{}", showId, totals, table);
        confirmedBySeat.forEach((seat, users) -> {
            if (users.size() > 1) {
                log.error("BookingLoadSimulator:: DOUBLE BOOKING! seat {} confirmed for {}", seat, users);
            } else {
                log.info("BookingLoadSimulator:: seat {} -> {} (only one winner)", seat, users.get(0));
            }
        });
        if (confirmedBySeat.values().stream().allMatch(users -> users.size() == 1)) {
            log.info("BookingLoadSimulator:: ✓ no seat was confirmed twice");
        }
    }

    private String statusOf(String reservationId) {
        try {
            return bookingClient.get()
                    .uri("/booking-service/bookings/{id}", reservationId)
                    .retrieve()
                    .body(BookingResponse.class)
                    .status();
        } catch (Exception e) {
            return "UNKNOWN";
        }
    }

    private record Attempt(String user, String seat, long amount, Chaos chaos, int attempts,
                           String reservationId, String error, boolean sentToDlt) {
    }

    /** Payload published to booking-request-dlt: everything needed to inspect or replay the request. */
    public record FailedBookingRequest(String userId, String showId, List<String> seatIds, long amount,
                                       String idempotencyKey, int attempts, String lastError, String reason,
                                       String chaos, Instant failedAt) {
    }
}
