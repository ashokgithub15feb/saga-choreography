package com.javatechie;

import com.javatechie.events.BookingPaymentEvent;
import com.javatechie.events.SeatReservedEvent;
import com.javatechie.exception.PoisonMessageException;
import com.javatechie.service.BookingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import static com.javatechie.common.KafkaConfigProperties.MOVIE_BOOKING_GROUP;
import static com.javatechie.common.KafkaConfigProperties.PAYMENT_EVENTS_TOPIC;
import static com.javatechie.common.KafkaConfigProperties.SEAT_RESERVED_TOPIC;

@Component
@Slf4j
public class MovieBookingListener {


    private BookingService service;

    public MovieBookingListener(BookingService service) {
        this.service = service;
    }

    @KafkaListener(topics = SEAT_RESERVED_TOPIC, groupId = MOVIE_BOOKING_GROUP)
    public void consumeSeatReserveEvents(SeatReservedEvent event, Acknowledgment ack){

        log.info("MovieBookingListener:: Consuming seatReserved event");

        if (event.bookingId() == null || event.bookingId().isBlank()) {
            throw new PoisonMessageException("SeatReservedEvent without bookingId: " + event);
        }

        if(event.reserved()){
            // seats locked; the booking is confirmed only when the payment result arrives
            log.info("Seats locked for bookingId: {}", event.bookingId());
            service.handleBookingOnSeatsLocked(event.bookingId());
        }else{
            //rollback
            log.info("Seat reservation failed for bookingId: {}", event.bookingId());
            service.handleBookingOnSeatReservationFailure(event.bookingId());
        }
        // only reached on success; an exception goes to the DefaultErrorHandler (retry / DLT) instead
        ack.acknowledge();
    }

    // Last step of the saga: payment result -> booking CONFIRMED or FAILED
    @KafkaListener(topics = PAYMENT_EVENTS_TOPIC, groupId = MOVIE_BOOKING_GROUP)
    public void consumePaymentEvents(BookingPaymentEvent event, Acknowledgment ack) {

        log.info("MovieBookingListener:: Consuming payment event for bookingId {}", event.bookingId());

        if (event.bookingId() == null || event.bookingId().isBlank()) {
            throw new PoisonMessageException("BookingPaymentEvent without bookingId: " + event);
        }

        if (event.paymentCompleted()) {
            service.handleBookingOnPaymentCompleted(event.bookingId());
        } else {
            service.handleBookingOnPaymentFailed(event.bookingId());
        }
        ack.acknowledge();
    }
}
