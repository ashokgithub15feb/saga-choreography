package com.javatechie.listener;

import com.javatechie.events.SeatReservedEvent;
import com.javatechie.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import static com.javatechie.common.KafkaConfigProperties.PAYMENT_EVENT_GROUP;
import static com.javatechie.common.KafkaConfigProperties.SEAT_RESERVED_TOPIC;

@Slf4j
@Service
@RequiredArgsConstructor
public class SeatReserveEventConsumer {

    private final PaymentService paymentService;


    // Read -> process -> write runs in one Kafka transaction (started by the container):
    // the consumed offset and the published payment event commit together or not at all (exactly-once).
    // No try/catch: exceptions must reach the DefaultErrorHandler (retry with backoff / DLT).
    @KafkaListener(topics = SEAT_RESERVED_TOPIC, groupId = PAYMENT_EVENT_GROUP)
    public void consume(SeatReservedEvent event, Acknowledgment ack) {
        log.info("Consumed SeatReservedEvent for bookingId: {} and event {}", event.bookingId(), event);
        if(event.reserved()) {
            paymentService.processPayment(event);
        }else{
            log.info("skipping payment processing as seat reservation failed for bookingId: {}", event.bookingId());
        }
        ack.acknowledge();
    }
}
