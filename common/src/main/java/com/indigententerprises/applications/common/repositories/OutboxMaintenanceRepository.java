package com.indigententerprises.applications.common.repositories;

import com.indigententerprises.applications.common.domain.OutboxRecord;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface OutboxMaintenanceRepository extends JpaRepository<OutboxRecord, Long> {
    @Modifying
    @Query(value = """
            WITH doomed AS (
              SELECT outbox_id
                FROM operations.outbox
               WHERE status = 'DELIVERED'
                 AND updated_at < now() - make_interval(days => :retentionDays)
            ORDER BY updated_at, outbox_id
              LIMIT :batchSize
            )
            DELETE FROM operations.outbox
            USING doomed
            WHERE outbox_id = doomed.outbox_id
                   """, nativeQuery = true)
    int purgeDeliveredBatch(
            @Param("retentionDays") int retentionDays,
            @Param("batchSize") int batchSize
    );

    @Modifying
    @Query(value = """
            WITH doomed AS (
              SELECT outbox_id
                FROM operations.outbox
               WHERE status = 'DEAD'
                 AND updated_at < now() - make_interval(days => :retentionDays)
            ORDER BY updated_at, outbox_id
               LIMIT :batchSize
            )
            DELETE FROM operations.outbox
            USING doomed
            WHERE outbox_id = doomed.outbox_id
           """, nativeQuery=true)
    int purgeDeadBatch(
            @Param("retentionDays") int retentionDays,
            @Param("batchSize") int batchSize
    );
}
