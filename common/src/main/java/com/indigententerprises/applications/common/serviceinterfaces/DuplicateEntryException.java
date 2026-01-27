package com.indigententerprises.applications.common.serviceinterfaces;

public class DuplicateEntryException extends Exception {
    public DuplicateEntryException(final String message) {
        super(message);
    }

    public DuplicateEntryException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
