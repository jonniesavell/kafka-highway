package com.indigententerprises.applications.common.domain;

public enum OutboxStatus {
    PENDING,
    DELIVERED,
    FAILED,
    DEAD
}
