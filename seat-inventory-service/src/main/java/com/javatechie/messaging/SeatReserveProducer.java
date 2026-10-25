package com.javatechie.messaging;

import com.javatechie.common.KafkaConfigProperties;
import com.javatechie.events.BookingCreatedEvent;
import com.javatechie.events.SeatReservedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import static com.javatechie.common.KafkaConfigProperties.SEAT_RESERVED_TOPIC;

@Component
@Slf4j
public class SeatReserveProducer {

    private KafkaTemplate<String, SeatReservedEvent> template;

    public SeatReserveProducer(KafkaTemplate<String, SeatReservedEvent> template) {
        this.template = template;
    }

    // No try/catch: a failed send must abort the listener's Kafka transaction (and be retried), not be swallowed
    public void publishSeatReserveEvents(SeatReservedEvent reservedEvent) {
        log.info("SeatReserveProducer:: Publishing seatReserved event for bookingId {}", reservedEvent.bookingId());
        template.send(SEAT_RESERVED_TOPIC,reservedEvent.bookingId(), reservedEvent);
    }
}
