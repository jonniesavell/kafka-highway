package com.indigententerprises.applications.common.serviceinterfaces;

public interface RelayOutboxService {
    boolean executeBatch() throws RuntimeException;
}
