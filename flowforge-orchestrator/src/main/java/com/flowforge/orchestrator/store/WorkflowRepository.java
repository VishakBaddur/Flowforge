package com.flowforge.orchestrator.store;

import com.flowforge.common.event.WorkflowEvent;
import com.flowforge.common.state.Decision;
import com.flowforge.common.state.WorkflowState;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/** Loads aggregates by replaying their events, and saves decider output. */
@Repository
public class WorkflowRepository {

    private final EventStore store;

    public WorkflowRepository(EventStore store) {
        this.store = store;
    }

    public Optional<WorkflowState> load(String workflowId) {
        List<WorkflowEvent> events = store.load(workflowId);
        return events.isEmpty() ? Optional.empty() : Optional.of(WorkflowState.replay(workflowId, events));
    }

    public void save(WorkflowState state, Decision decision) {
        if (!decision.isIgnored()) store.append(state, decision.events());
    }
}
