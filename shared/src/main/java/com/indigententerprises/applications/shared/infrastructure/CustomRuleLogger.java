package com.indigententerprises.applications.shared.infrastructure;

import org.jsonschema2pojo.RuleLogger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomRuleLogger implements RuleLogger {

    private static final Logger log = LoggerFactory.getLogger(CustomRuleLogger.class);

    @Override
    public void debug(String s) {
        log.debug(s);
    }

    @Override
    public void error(String s) {
        log.error(s);
    }

    @Override
    public void error(String s, Throwable throwable) {
        log.error(s, throwable);
    }

    @Override
    public void info(String s) {
        log.info(s);
    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public boolean isErrorEnabled() {
        return true;
    }

    @Override
    public boolean isInfoEnabled() {
        return true;
    }

    @Override
    public boolean isTraceEnabled() {
        return false;
    }

    @Override
    public boolean isWarnEnabled() {
        return true;
    }

    @Override
    public void trace(String s) {
        log.trace(s);
    }

    @Override
    public void warn(String s, Throwable throwable) {
        log.warn(s, throwable);
    }

    @Override
    public void warn(String s) {
        log.warn(s);
    }
}
