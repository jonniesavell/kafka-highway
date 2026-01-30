package com.indigententerprises.applications.common.infrastructure;

import com.indigententerprises.applications.common.serviceinterfaces.OutboxCleanupService;
import com.indigententerprises.applications.common.serviceinterfaces.RelayOutboxService;

import org.springframework.beans.BeansException;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.annotation.Scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OutboxRecordPoller implements Runnable, ApplicationContextAware {

    private static final Logger log = LoggerFactory.getLogger(OutboxRecordPoller.class);

    private final RelayOutboxService relayOutboxService;
    private final OutboxCleanupService outboxCleanupService;
    private final long maxWaitTime;
    private final int maxNumberOfExceptions;
    private final Lock lock;

    private ApplicationContext applicationContext;

    public OutboxRecordPoller(
            final RelayOutboxService relayOutboxService,
            final OutboxCleanupService outboxCleanupService,
            final int maxNumberOfExceptions
    ) {
        this.relayOutboxService = relayOutboxService;
        this.outboxCleanupService = outboxCleanupService;
        this.maxWaitTime = 60L;
        this.maxNumberOfExceptions = maxNumberOfExceptions;
        this.lock = new ReentrantLock();
    }

    @Override
    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void run() {

        // mutable data
        long backOff = 1L;
        int numberOfExceptions = 0;

        try {
            while (!Thread.currentThread().isInterrupted()) {
                // mutable data
                boolean workDone;

                lock.lock();

                try {
                    workDone = relayOutboxService.executeBatch();
                    numberOfExceptions = 0;
                } catch (RuntimeException e) {
                    log.error("runtime exception caught in OutboxRecordPoller", e);

                    workDone = false;
                    numberOfExceptions++;
                } finally {
                    lock.unlock();
                }

                if (numberOfExceptions >= maxNumberOfExceptions) {
                    throw new RuntimeException("maximum number of exceptions exceeded");
                } else {
                    if (workDone) {
                        backOff = 1;
                    } else {
                        try {
                            Thread.sleep(backOff * 1000);
                            backOff = Math.min(maxWaitTime, backOff * 2);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
        } catch (RuntimeException e) {
            SpringApplication.exit(applicationContext, () -> 1);
            System.exit(1);
        }
    }

    @Scheduled(fixedRate=24, timeUnit=TimeUnit.HOURS)
    public void cleanup() {
        final int batchSize = 10_000;

        // mutable data
        int numberPurged;

        try {
            do {
                lock.lock();

                try {
                    numberPurged = outboxCleanupService.purgeDeliveredBatch(7, batchSize);
                } finally {
                    lock.unlock();
                }
            } while (numberPurged > 0);
        } catch (RuntimeException e) {
            log.error("runtime exception caught in cleanup of delivered messages", e);
        }

        try {
            do {
                lock.lock();

                try {
                    numberPurged = outboxCleanupService.purgeDeadBatch(90, batchSize);
                } finally {
                    lock.unlock();
                }
            } while (numberPurged > 0);
        } catch (RuntimeException e) {
            log.error("runtime exception caught in cleanup of dead messages", e);
        }
    }
}
