package com.javatechie.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.javatechie.common.KafkaConfigProperties.DLT_SUFFIX;
import static com.javatechie.common.KafkaConfigProperties.MOVIE_BOOKING_EVENTS_TOPIC;
import static com.javatechie.common.KafkaConfigProperties.PAYMENT_EVENTS_TOPIC;
import static com.javatechie.common.KafkaConfigProperties.SEAT_EVENT_GROUP;

/**
 * Reads the dead letter topics this service publishes to and logs every failed record
 * with the metadata DeadLetterPublishingRecoverer put in its headers (where it came from and why it failed).
 * The value is read as a plain String: a DLT can hold any payload, including JSON that never deserialized.
 * This listener only logs and never throws, so a DLT record can't fail again.
 */
@Component
@Slf4j
public class DeadLetterListener {

    @KafkaListener(topics = {MOVIE_BOOKING_EVENTS_TOPIC + DLT_SUFFIX, PAYMENT_EVENTS_TOPIC + DLT_SUFFIX},
            groupId = "${spring.application.name}-dlt-group",
            properties = {
                    "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                    // a new DLT group also reads failures recorded before it first started
                    "auto.offset.reset=earliest"
            })
    public void consumeFailedRecord(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String originalGroup = stringHeader(record, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP);
        // payment-events-dlt is shared with booking-service's consumer group: only log the records this service failed
        if (!SEAT_EVENT_GROUP.equals(originalGroup)) {
            ack.acknowledge();
            return;
        }

        log.error("DLT record {}-{}@{} | original {}-{}@{} group={} | key={} | exception={} cause={} message={} | value={}",
                record.topic(), record.partition(), record.offset(),
                stringHeader(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                intHeader(record, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                longHeader(record, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                originalGroup,
                record.key(),
                stringHeader(record, KafkaHeaders.DLT_EXCEPTION_FQCN),
                stringHeader(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN),
                stringHeader(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                record.value());
        ack.acknowledge();
    }

    private static String stringHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    // partition is written as a 4-byte int, offset as an 8-byte long
    private static Integer intHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : ByteBuffer.wrap(header.value()).getInt();
    }

    private static Long longHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : ByteBuffer.wrap(header.value()).getLong();
    }
}
