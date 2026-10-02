package com.flowforge.api.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import org.hibernate.annotations.Immutable;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/** Read-only JPA view of the append-only event log. */
@Entity
@Immutable
@Table(name = "workflow_events")
@IdClass(WorkflowEventEntity.Key.class)
public class WorkflowEventEntity {

    @Id
    @Column(name = "workflow_id")
    private String workflowId;
    @Id
    private long sequence;
    @Column(name = "event_type")
    private String eventType;
    @Column(columnDefinition = "jsonb")
    private String payload;
    @Column(name = "created_at")
    private Instant createdAt;

    protected WorkflowEventEntity() {
    }

    public String getWorkflowId() { return workflowId; }
    public long getSequence() { return sequence; }
    public String getEventType() { return eventType; }
    public String getPayload() { return payload; }
    public Instant getCreatedAt() { return createdAt; }

    public static class Key implements Serializable {
        private String workflowId;
        private long sequence;

        public Key() {
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Key k && sequence == k.sequence && Objects.equals(workflowId, k.workflowId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(workflowId, sequence);
        }
    }
}
