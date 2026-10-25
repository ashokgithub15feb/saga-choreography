package com.javatechie.service;

import com.javatechie.entity.Booking;
import com.javatechie.entity.ProcessedEvent;
import com.javatechie.events.BookingCreatedEvent;
import com.javatechie.exception.BookingNotFoundException;
import com.javatechie.exception.IdempotencyKeyReuseException;
import com.javatechie.exception.PoisonMessageException;
import com.javatechie.messaging.BookingEventProducer;
import com.javatechie.repository.BookingRepository;
import com.javatechie.repository.ProcessedEventRepository;
import com.javatechie.request.BookingRequest;
import com.javatechie.response.BookingResponse;
import com.javatechie.utils.mapper.EntityToBookingResponseMapper;
import com.javatechie.utils.mapper.BookingRequestToEntityMapper;
import com.javatechie.utils.enums.BookingStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Objects;


@Service
@Slf4j
public class BookingService {

    private final BookingRepository bookingRepository;

    private final BookingEventProducer bookingEventProducer;

    private final ProcessedEventRepository processedEventRepository;

    public BookingService(BookingRepository bookingRepository, BookingEventProducer bookingEventProducer,
                          ProcessedEventRepository processedEventRepository) {
        this.bookingEventProducer = bookingEventProducer;
        this.bookingRepository = bookingRepository;
        this.processedEventRepository = processedEventRepository;
    }

    /**
     * Reserves seats for a given show and user.
     * Validates the request, maps it to an entity, persists it and returns a response DTO.
     *
     * @param request the seat reservation request
     * @return SeatReserveResponse containing reservation details
     * @throws IllegalArgumentException if the request is invalid
     * <p>
     * One DB transaction: booking row + outbox row (BookingCreatedEvent). No Kafka call in the request path;
     * OutboxRelay publishes the event after commit, so a Kafka outage can't lose it or fail the request.
     * <p>
     * Idempotent per Idempotency-Key: a retry with the same key returns the booking created the first time.
     */
    @Transactional
    public BookingResponse bookSeats(BookingRequest request, String idempotencyKey) {

        var existing = bookingRepository.findByIdempotencyKey(idempotencyKey);
        if (existing.isPresent()) {
            return replay(existing.get(), request, idempotencyKey);
        }

        log.info("Booking seats for user {} for show {}", request.userId(), request.showId());

        // Map request -> entity
        var reservationEntity = BookingRequestToEntityMapper.map(request);
        reservationEntity.setIdempotencyKey(idempotencyKey);

        // Persist (flush now, so a concurrent request with the same key fails here on the unique constraint
        // -> DataIntegrityViolationException -> 409 via GlobalExceptionHandler)
        var savedReservation = bookingRepository.saveAndFlush(reservationEntity);

        // Booking created event -> outbox, same transaction
        var bookingCreatedEvent = buildBookingCreateEvents(savedReservation);
        bookingEventProducer.publishBookingEvents(bookingCreatedEvent);

        var response = EntityToBookingResponseMapper.map(savedReservation);

        log.info("Booking {} created as PENDING, saga started", response.reservationId());
        return response;
    }

    // Current saga status of a booking (POST returns PENDING; the saga finishes asynchronously)
    @Transactional(readOnly = true)
    public BookingResponse getBooking(String bookingId) {
        var booking = bookingRepository.findByBookingCode(bookingId);
        if (booking == null) {
            throw new BookingNotFoundException(bookingId);
        }
        return EntityToBookingResponseMapper.map(booking);
    }

    // Same key + same request -> return the original booking (current status); same key + other request -> 422
    private BookingResponse replay(Booking booking, BookingRequest request, String idempotencyKey) {
        boolean sameRequest = Objects.equals(booking.getUserId(), request.userId())
                && Objects.equals(booking.getShowId(), request.showId())
                && Objects.equals(new ArrayList<>(booking.getSeatIds()), request.seatIds())
                && booking.getAmount() == request.amount();
        if (!sameRequest) {
            throw new IdempotencyKeyReuseException(idempotencyKey);
        }
        log.info("Idempotency-Key {} already used -> returning booking {}", idempotencyKey, booking.getBookingCode());
        return EntityToBookingResponseMapper.map(booking);
    }

    private BookingCreatedEvent buildBookingCreateEvents(Booking savedReservation) {
        return new BookingCreatedEvent(savedReservation.getBookingCode(), savedReservation.getUserId(), savedReservation.getShowId(), savedReservation.getSeatIds(), savedReservation.getAmount());
    }


    // ---- saga events -> booking state machine (each one idempotent, see moveBookingTo) ----

    @Transactional
    public void handleBookingOnSeatsLocked(String bookingId) {
        moveBookingTo(bookingId, BookingStatus.SEATS_LOCKED, "SEATS_LOCKED:" + bookingId);
    }

    // seats not available, released after payment failure, or seat lock expired
    @Transactional
    public void handleBookingOnSeatReservationFailure(String bookingId) {
        moveBookingTo(bookingId, BookingStatus.FAILED, "SEAT_RESERVATION_FAILED:" + bookingId);
    }

    @Transactional
    public void handleBookingOnPaymentCompleted(String bookingId) {
        moveBookingTo(bookingId, BookingStatus.CONFIRMED, "PAYMENT_COMPLETED:" + bookingId);
    }

    @Transactional
    public void handleBookingOnPaymentFailed(String bookingId) {
        moveBookingTo(bookingId, BookingStatus.FAILED, "PAYMENT_FAILED:" + bookingId);
    }

    /*
     * Status update + idempotency record are saved in one DB transaction.
     * - duplicate event (already in processed_event) -> ignored
     * - transition not allowed by the state machine (out-of-order / late event) -> ignored, but recorded
     */
    private void moveBookingTo(String bookingId, BookingStatus target, String eventKey) {
        if (processedEventRepository.existsById(eventKey)) {
            log.warn("BookingService:: Duplicate event {} ignored", eventKey);
            return;
        }

        var bookingDetails=bookingRepository.findByBookingCode(bookingId);
        if(bookingDetails==null){
            // bad data: retrying will never find it -> straight to DLT
            throw new PoisonMessageException("No booking found with bookingId " + bookingId);
        }

        BookingStatus current = bookingDetails.getStatus();
        if (current.canMoveTo(target)) {
            bookingDetails.setStatus(target);
            bookingRepository.save(bookingDetails);
            log.info("BookingService:: Booking {} moved {} -> {}", bookingId, current, target);
        } else {
            log.warn("BookingService:: Booking {} ignored transition {} -> {} (event {})", bookingId, current, target, eventKey);
        }
        processedEventRepository.save(new ProcessedEvent(eventKey, Instant.now()));
    }

    /*
     * Saga timeout on the booking side: a booking still PENDING after the timeout never got an answer from
     * seat-inventory (e.g. its BookingCreatedEvent ended up in the DLT), so it is failed.
     * Called by BookingTimeoutScheduler; re-checks the status inside its own transaction.
     */
    @Transactional
    public void expirePendingBooking(String bookingId) {
        var booking = bookingRepository.findByBookingCode(bookingId);
        if (booking != null && booking.getStatus() == BookingStatus.PENDING) {
            booking.setStatus(BookingStatus.FAILED);
            bookingRepository.save(booking);
            log.warn("BookingService:: Booking {} still PENDING after timeout -> FAILED", bookingId);
        }
    }
}
