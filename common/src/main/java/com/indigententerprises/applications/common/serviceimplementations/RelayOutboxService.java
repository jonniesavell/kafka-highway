package com.indigententerprises.applications.common.serviceimplementations;

import com.indigententerprises.applications.common.repositories.OutboxRepository;
import com.indigententerprises.applications.common.domain.OutboxRecord;

import org.springframework.transaction.annotation.Transactional;


public class RelayOutboxService
{ //implements com.indigententerprises.applications.common.serviceinterfaces.RelayOutboxService {

    private final OutboxRepository outboxRepository;

    public RelayOutboxService(final OutboxRepository outboxRepository) {
        this.outboxRepository = outboxRepository;
    }

    @Transactional
    public OutboxRecord update(final OutboxRecord record) {
        // TODO: gimme the meat
        return outboxRepository.saveAndFlush(record);
    }
}
