package com.javatechie.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.javatechie.entity.OutboxEvent;
import com.javatechie.events.BookingCreatedEvent;
import com.javatechie.repository.OutboxEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import static com.javatechie.common.KafkaConfigProperties.MOVIE_BOOKING_EVENTS_TOPIC;

/**
 * "Publishes" by writing to the outbox table inside the caller's DB transaction (no Kafka call here).
 * The booking row and its event are committed atomically; OutboxRelay sends the event to Kafka afterwards.
 */
@Component
@Slf4j
public class BookingEventProducer {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public BookingEventProducer(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    // MANDATORY: writing an outbox row outside the business transaction would defeat the pattern
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishBookingEvents(BookingCreatedEvent createdEvent) {
        log.info("Saving booking event to outbox for id : {}", createdEvent.bookingId());
        try {
            outboxEventRepository.save(new OutboxEvent(createdEvent.bookingId(), MOVIE_BOOKING_EVENTS_TOPIC,
                    createdEvent.getClass().getName(), objectMapper.writeValueAsString(createdEvent)));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize " + createdEvent, e);
        }
    }
}
