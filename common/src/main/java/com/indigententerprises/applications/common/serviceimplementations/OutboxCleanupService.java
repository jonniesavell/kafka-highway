package com.indigententerprises.applications.common.serviceimplementations;

import com.indigententerprises.applications.common.repositories.OutboxMaintenanceRepository;

import org.springframework.transaction.annotation.Transactional;

public class OutboxCleanupService
        implements com.indigententerprises.applications.common.serviceinterfaces.OutboxCleanupService {
    private final OutboxMaintenanceRepository outboxMaintenanceRepository;

    public OutboxCleanupService(
            final OutboxMaintenanceRepository outboxMaintenanceRepository
    ) {
        this.outboxMaintenanceRepository = outboxMaintenanceRepository;
    }

    @Transactional
    public int purgeDeliveredBatch(final int retentionDays, final int batchSize) {
        return outboxMaintenanceRepository.purgeDeliveredBatch(retentionDays, batchSize);
    }

    @Transactional
    public int purgeDeadBatch(final int retentionDays, final int batchSize) {
        return outboxMaintenanceRepository.purgeDeadBatch(retentionDays, batchSize);
    }
}
