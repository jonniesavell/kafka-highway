package com.indigententerprises.applications.common.serviceinterfaces;

public interface OutboxCleanupService {
    int purgeDeliveredBatch(final int retentionDays, final int batchSize);

    int purgeDeadBatch(final int retentionDays, final int batchSize);
}
