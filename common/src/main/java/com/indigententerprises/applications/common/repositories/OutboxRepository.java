package com.indigententerprises.applications.common.repositories;

import com.indigententerprises.applications.common.domain.OutboxRecord;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OutboxRepository extends JpaRepository<OutboxRecord, Long> {
    @Query("""
           SELECT o
             FROM OutboxRecord o
            WHERE o.status = :status
              AND o.attemptCount <= :maxAttempts
         ORDER BY o.createdAt ASC, o.id ASC
           """)
    Page<OutboxRecord> findByCriteria(
            @Param("status") String status,
            @Param("maxAttempts") int maxAttempts,
            Pageable pageable
    );
}
