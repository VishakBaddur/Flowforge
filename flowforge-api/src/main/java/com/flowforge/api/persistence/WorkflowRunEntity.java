package com.flowforge.api.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.Immutable;

import java.time.Instant;

/** Read-only JPA view of the orchestrator's workflow_runs read model. */
@Entity
@Immutable
@Table(name = "workflow_runs")
public class WorkflowRunEntity {

    @Id
    @Column(name = "workflow_id")
    private String workflowId;
    private String name;
    private String status;
    private long version;
    @Column(name = "created_at")
    private Instant createdAt;
    @Column(name = "updated_at")
    private Instant updatedAt;

    protected WorkflowRunEntity() {
    }

    public String getWorkflowId() { return workflowId; }
    public String getName() { return name; }
    public String getStatus() { return status; }
    public long getVersion() { return version; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
