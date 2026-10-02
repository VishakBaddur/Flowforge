package com.flowforge.api.service;

import com.flowforge.api.persistence.WorkflowEventRepository;
import com.flowforge.api.persistence.WorkflowRunRepository;
import com.flowforge.api.web.ApiModels.EventView;
import com.flowforge.api.web.ApiModels.PageResponse;
import com.flowforge.api.web.ApiModels.WorkflowSummary;
import com.flowforge.api.web.ApiModels.WorkflowView;
import com.flowforge.common.dag.DagValidator;
import com.flowforge.common.event.EventCodec;
import com.flowforge.common.event.WorkflowEvent;
import com.flowforge.common.messaging.Topics;
import com.flowforge.common.messaging.WorkflowCommand;
import com.flowforge.common.model.WorkflowDefinition;
import com.flowforge.common.state.WorkflowState;
import com.flowforge.common.tracing.KafkaTraceContext;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class WorkflowService {

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;
    private final EventCodec codec;
    private final WorkflowRunRepository runs;
    private final WorkflowEventRepository events;
    private final Clock clock;
    private final KafkaTraceContext trace;

    public WorkflowService(KafkaTemplate<String, String> kafka, JsonMapper json, EventCodec codec,
                           WorkflowRunRepository runs, WorkflowEventRepository events, Clock clock,
                           KafkaTraceContext trace) {
        this.trace = trace;
        this.kafka = kafka;
        this.json = json;
        this.codec = codec;
        this.runs = runs;
        this.events = events;
        this.clock = clock;
    }

    /** Validates synchronously, then hands off to the orchestrator. Same idempotency key => same workflow id. */
    public String submit(WorkflowDefinition definition, String idempotencyKey, String owner) {
        DagValidator.validate(definition);
        String workflowId = idempotencyKey == null || idempotencyKey.isBlank()
                ? "wf-" + UUID.randomUUID()
                : "wf-" + UUID.nameUUIDFromBytes((owner + ":" + idempotencyKey).getBytes(StandardCharsets.UTF_8));
        publish(workflowId, WorkflowCommand.start(workflowId, definition, owner, clock.instant()));
        return workflowId;
    }

    /** Caller must have checked visibility (see WorkflowController). */
    public void cancel(String workflowId, String reason) {
        publish(workflowId, WorkflowCommand.cancel(workflowId, reason, clock.instant()));
    }

    /** Terminal workflows never change, so only those are cached. */
    @Cacheable(cacheNames = "workflows", key = "#workflowId", unless = "!#result.terminal()")
    public WorkflowView get(String workflowId) {
        List<WorkflowEvent> log = events.findByWorkflowIdOrderBySequence(workflowId).stream()
                .map(e -> codec.decode(e.getEventType(), e.getPayload()))
                .toList();
        if (log.isEmpty()) throw new WorkflowNotFoundException(workflowId);
        return WorkflowView.from(WorkflowState.replay(workflowId, log));
    }

    /** owner == null means "all owners" (admins only). */
    public PageResponse<WorkflowSummary> list(String owner, String status, Pageable pageable) {
        String s = status == null ? null : status.toUpperCase();
        Page<WorkflowSummary> page = (owner == null
                ? (s == null ? runs.findAll(pageable) : runs.findByStatus(s, pageable))
                : (s == null ? runs.findByOwner(owner, pageable) : runs.findByOwnerAndStatus(owner, s, pageable)))
                .map(WorkflowSummary::from);
        return new PageResponse<>(page.getContent(), page.getNumber(), page.getSize(),
                page.getTotalElements(), page.getTotalPages());
    }

    public List<EventView> events(String workflowId) {
        List<EventView> views = events.findByWorkflowIdOrderBySequence(workflowId).stream()
                .map(e -> new EventView(e.getSequence(), e.getEventType(), e.getCreatedAt(), json.readTree(e.getPayload())))
                .toList();
        if (views.isEmpty()) throw new WorkflowNotFoundException(workflowId);
        return views;
    }

    private void publish(String workflowId, WorkflowCommand command) {
        try {
            // Wait for the broker ack so "202 Accepted" really means the command is durable.
            kafka.send(trace.record(Topics.WORKFLOW_COMMANDS, workflowId, json.writeValueAsString(command), null))
                    .get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while publishing", e);
        } catch (Exception e) {
            throw new CommandPublishException(e);
        }
    }

    public static class CommandPublishException extends RuntimeException {
        CommandPublishException(Throwable cause) {
            super("could not hand the command to Kafka", cause);
        }
    }
}
