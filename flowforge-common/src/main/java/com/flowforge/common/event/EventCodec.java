package com.flowforge.common.event;

import tools.jackson.databind.json.JsonMapper;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/** JSON <-> WorkflowEvent. The type registry is derived from the sealed interface, so new events need no wiring. */
public final class EventCodec {

    private static final Map<String, Class<? extends WorkflowEvent>> TYPES =
            Arrays.stream(WorkflowEvent.class.getPermittedSubclasses())
                    .collect(Collectors.toUnmodifiableMap(Class::getSimpleName, c -> c.asSubclass(WorkflowEvent.class)));

    private final JsonMapper json;

    public EventCodec(JsonMapper json) {
        this.json = json;
    }

    public String encode(WorkflowEvent event) {
        return json.writeValueAsString(event);
    }

    public WorkflowEvent decode(String type, String payload) {
        Class<? extends WorkflowEvent> clazz = TYPES.get(type);
        if (clazz == null) throw new IllegalStateException("unknown event type '" + type + "'");
        return json.readValue(payload, clazz);
    }
}
