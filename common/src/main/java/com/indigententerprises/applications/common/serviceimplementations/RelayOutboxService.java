package com.indigententerprises.applications.common.serviceimplementations;

import com.indigententerprises.applications.common.repositories.OutboxRepository;
import com.indigententerprises.applications.common.domain.DestinationKind;
import com.indigententerprises.applications.common.domain.OutboxRecord;
import com.indigententerprises.applications.common.domain.OutboxStatus;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class RelayOutboxService
        implements com.indigententerprises.applications.common.serviceinterfaces.RelayOutboxService {
    private final OfframpPublisher offrampPublisher;
    private final OutboxRepository outboxRepository;
    private final DltPublisher dltPublisher;
    private final TransactionTemplate transactionTemplate;

    public RelayOutboxService(
            final OfframpPublisher offrampPublisher,
            final DltPublisher dltPublisher,
            final OutboxRepository outboxRepository,
            final PlatformTransactionManager transactionManager
    ) {
        this.offrampPublisher = offrampPublisher;
        this.dltPublisher = dltPublisher;
        this.outboxRepository = outboxRepository;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
    }

    public void execute() {

        final int batchSize = 100;

        List<OutboxRecord> results;

        do {
            final Instant now = Instant.now();
            results = transactionTemplate.execute(
                    new TransactionCallback<List<OutboxRecord>>() {
                        @Override
                        public List<OutboxRecord> doInTransaction(final TransactionStatus status) {
                            final Pageable pageable = PageRequest.of(0, batchSize);
                            final Page<OutboxRecord> page = outboxRepository.findByCriteria(
                                    OutboxStatus.PENDING.name(),
                                    3,
                                    pageable
                            );
                            return page.getContent();
                        }
                    }
            );

            if (results == null) {
                results = Collections.emptyList();
            } else {
                if (!results.isEmpty()) {
                    final ArrayList<OutboxRecord> toUpdate = new ArrayList<>(results.size());

                    for (final OutboxRecord outboxRecord : results) {
                        final boolean success = handleRecord(outboxRecord);
                        outboxRecord.setUpdatedAt(now);

                        if (success) {
                            outboxRecord.setStatus(OutboxStatus.DELIVERED.name());
                        } else {
                            int attemptCount = outboxRecord.getAttemptCount() + 1;

                            if (attemptCount > 3) {
                                outboxRecord.setStatus(OutboxStatus.DEAD.name());
                            }

                            outboxRecord.setAttemptCount(attemptCount);
                        }

                        toUpdate.add(outboxRecord);
                    }

                    transactionTemplate.executeWithoutResult(new Consumer<TransactionStatus>() {
                        @Override
                        public void accept(final TransactionStatus status) {
                            outboxRepository.saveAll(toUpdate);
                        }
                    });
                }
            }
        } while (!results.isEmpty());
    }

    private boolean handleRecord(final OutboxRecord outboxRecord) {
        final String key = outboxRecord.getDestinationKey();
        final String json = outboxRecord.getEnvelopeJson();

        if (DestinationKind.DLT.name().equals(outboxRecord.getDestinationKind())) {
            try {
                dltPublisher.publishBlocking(
                        key,
                        json,
                        outboxRecord.getErrorKind(),
                        outboxRecord.getErrorDetail(),
                        outboxRecord.getSourceTopic(),
                        outboxRecord.getSourcePartition(),
                        outboxRecord.getSourceOffset()
                );
                return true;
            } catch (ExecutionException | InterruptedException e) {
                return false;
            }
        } else if (outboxRecord.getDestinationKind().equals(DestinationKind.OFFRAMP.name())) {
            try {
                offrampPublisher.send(outboxRecord);
                return true;
            } catch (Exception ex) {
                return false;
            }
        } else {
            return false;
        }
    }
}
