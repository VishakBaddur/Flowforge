package com.flowforge.common.state;

import com.flowforge.common.event.WorkflowEvent;

import java.util.List;

/** Result of a decider call: events to persist, or a reason the input was ignored (duplicate, stale, not due). */
public record Decision(List<WorkflowEvent> events, String ignoredReason) {

    public Decision {
        events = List.copyOf(events);
    }

    static Decision of(List<WorkflowEvent> events) {
        return new Decision(events, null);
    }

    static Decision ignored(String reason) {
        return new Decision(List.of(), reason);
    }

    public boolean isIgnored() {
        return ignoredReason != null;
    }
}
