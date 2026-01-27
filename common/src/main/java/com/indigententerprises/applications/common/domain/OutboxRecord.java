package com.indigententerprises.applications.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name="outbox", schema="operations")
public class OutboxRecord extends AuditedEntity {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    @Column(name="outbox_id")
    private String id;

    @Column(name="source_topic")
    private String sourceTopic;

    @Column(name="source_partition")
    private Integer sourcePartition;

    @Column(name="source_offset")
    private Long sourceOffset;

    @Column(name="destination_kind")
    private String destinationKind;

    @Column(name="destination_topic")
    private String destinationTopic;

    @Column(name="destination_key")
    private String destinationKey;

    @Column(name="event_type")
    private String eventType;

    @Column(name="version")
    private Integer version;

    @Column(name="envelope_json")
    private String envelopeJson;

    @Column(name="status")
    private String status;

    @Column(name="validation_ok")
    private Boolean validationOk;

    @Column(name="error_kind")
    private String errorKind;

    @Column(name="error_detail")
    private String errorDetail;

    @Column(name="attempt_count")
    private Integer attemptCount;

    @Column(name="last_attempted_at")
    private Instant lastAttemptedAt;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public void setSourceTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    public Integer getSourcePartition() {
        return sourcePartition;
    }

    public void setSourcePartition(Integer sourcePartition) {
        this.sourcePartition = sourcePartition;
    }

    public Long getSourceOffset() {
        return sourceOffset;
    }

    public void setSourceOffset(Long sourceOffset) {
        this.sourceOffset = sourceOffset;
    }

    public String getDestinationKind() {
        return destinationKind;
    }

    public void setDestinationKind(String destinationKind) {
        this.destinationKind = destinationKind;
    }

    public String getDestinationTopic() {
        return destinationTopic;
    }

    public void setDestinationTopic(String destinationTopic) {
        this.destinationTopic = destinationTopic;
    }

    public String getDestinationKey() {
        return destinationKey;
    }

    public void setDestinationKey(String destinationKey) {
        this.destinationKey = destinationKey;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public String getEnvelopeJson() {
        return envelopeJson;
    }

    public void setEnvelopeJson(String envelopeJson) {
        this.envelopeJson = envelopeJson;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Boolean getValidationOk() {
        return validationOk;
    }

    public void setValidationOk(Boolean validationOk) {
        this.validationOk = validationOk;
    }

    public String getErrorKind() {
        return errorKind;
    }

    public void setErrorKind(String errorKind) {
        this.errorKind = errorKind;
    }

    public String getErrorDetail() {
        return errorDetail;
    }

    public void setErrorDetail(String errorDetail) {
        this.errorDetail = errorDetail;
    }

    public Integer getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(Integer attemptCount) {
        this.attemptCount = attemptCount;
    }

    public Instant getLastAttemptedAt() {
        return lastAttemptedAt;
    }

    public void setLastAttemptedAt(Instant lastAttemptedAt) {
        this.lastAttemptedAt = lastAttemptedAt;
    }
}
