package com.javatechie.repository;

import com.javatechie.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {

    // oldest first, so events of one booking are published in the order they were written
    List<OutboxEvent> findTop100BySentAtIsNullOrderByCreatedAtAsc();

    @Modifying
    @Transactional
    @Query("update OutboxEvent o set o.sentAt = :sentAt where o.id in :ids")
    int markSent(@Param("ids") Collection<String> ids, @Param("sentAt") Instant sentAt);

    @Modifying
    @Transactional
    @Query("delete from OutboxEvent o where o.sentAt < :cutoff")
    int deleteSentBefore(@Param("cutoff") Instant cutoff);
}
