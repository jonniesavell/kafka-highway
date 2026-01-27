package com.indigententerprises.applications.common.serviceinterfaces;

import com.indigententerprises.applications.common.domain.OutboxRecord;

public interface RelayOutboxService {
    OutboxRecord update(final OutboxRecord record);
}
