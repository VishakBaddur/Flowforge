package com.flowforge.api.persistence;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WorkflowRunRepository extends JpaRepository<WorkflowRunEntity, String> {

    Page<WorkflowRunEntity> findByStatus(String status, Pageable pageable);

    Page<WorkflowRunEntity> findByOwner(String owner, Pageable pageable);

    Page<WorkflowRunEntity> findByOwnerAndStatus(String owner, String status, Pageable pageable);
}
