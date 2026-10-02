package com.flowforge.api.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface WorkflowEventRepository extends JpaRepository<WorkflowEventEntity, WorkflowEventEntity.Key> {

    List<WorkflowEventEntity> findByWorkflowIdOrderBySequence(String workflowId);
}
