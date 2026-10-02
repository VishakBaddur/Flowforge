package com.flowforge.orchestrator.store;

import com.flowforge.common.event.WorkflowEvent;
import com.flowforge.common.state.WorkflowState;

import java.util.List;

/** Events the decider already applied to {@code state}, waiting to be committed together with others. */
public record PendingAppend(WorkflowState state, List<WorkflowEvent> events) {}
