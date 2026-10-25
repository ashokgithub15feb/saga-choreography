package com.javatechie.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.javatechie.entity.OutboxEvent;
import com.javatechie.repository.OutboxEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Outbox relay (polling publisher): sends unsent outbox rows to Kafka, then marks them sent.
 * <p>
 * Order matters: Kafka transaction COMMITTED first, DB "sentAt" second.
 * - Kafka fails      -> rows stay unsent -> retried on the next poll (nothing lost)
 * - crash before the DB update -> rows are sent again -> duplicate, absorbed by the idempotent consumer
 *   (seat-inventory's processed_event check). So delivery is at-least-once, never "maybe zero".
 * With several instances each one polls the same rows (more duplicates, still correct); use
 * SELECT ... FOR UPDATE SKIP LOCKED or ShedLock to avoid that, or Debezium CDC instead of polling.
 */
@Component
@Slf4j
public class OutboxRelay {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Duration retention;

    public OutboxRelay(OutboxEventRepository outboxEventRepository, KafkaTemplate<String, Object> kafkaTemplate,
                       ObjectMapper objectMapper, @Value("${outbox.retention:P7D}") Duration retention) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.retention = retention;
    }

    @Scheduled(fixedDelayString = "${outbox.poll-interval:PT1S}")
    public void publishPendingEvents() {
        List<OutboxEvent> pending = outboxEventRepository.findTop100BySentAtIsNullOrderByCreatedAtAsc();
        if (pending.isEmpty()) {
            return;
        }

        // rebuild the event objects first, so the JsonSerializer adds the same type headers as before
        Map<OutboxEvent, Object> batch = new LinkedHashMap<>();
        for (OutboxEvent event : pending) {
            try {
                batch.put(event, objectMapper.readValue(event.getPayload(), Class.forName(event.getEventType())));
            } catch (Exception e) {
                // stays unsent and visible in the table; must be fixed by hand
                log.error("OutboxRelay:: Cannot rebuild outbox event {} ({}), skipped", event.getId(), event.getEventType(), e);
            }
        }
        if (batch.isEmpty()) {
            return;
        }

        try {
            // one Kafka transaction for the batch; commit throws if any send failed
            kafkaTemplate.executeInTransaction(ops -> {
                batch.forEach((event, payload) -> ops.send(event.getTopic(), event.getAggregateId(), payload));
                return null;
            });
        } catch (Exception e) {
            log.warn("OutboxRelay:: Publishing {} outbox event(s) failed, will retry: {}", batch.size(), e.getMessage());
            return;
        }

        List<String> sentIds = new ArrayList<>();
        batch.keySet().forEach(event -> sentIds.add(event.getId()));
        outboxEventRepository.markSent(sentIds, Instant.now());
        log.info("OutboxRelay:: Published {} outbox event(s)", sentIds.size());
    }

    // sent rows are only kept for troubleshooting
    @Scheduled(fixedDelayString = "${outbox.cleanup-interval:PT1H}")
    public void deleteOldSentEvents() {
        int deleted = outboxEventRepository.deleteSentBefore(Instant.now().minus(retention));
        if (deleted > 0) {
            log.info("OutboxRelay:: Deleted {} sent outbox event(s) older than {}", deleted, retention);
        }
    }
}
