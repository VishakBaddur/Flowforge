package com.flowforge.orchestrator.store;

import com.flowforge.common.event.WorkflowEvent;
import com.flowforge.common.state.WorkflowState;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

@Repository
public class EventStore {

    private static final String INSERT_EVENT = """
            INSERT INTO workflow_events (workflow_id, sequence, event_type, payload, created_at)
            VALUES (?, ?, ?, ?::jsonb, ?)
            """;

    private static final String UPSERT_RUN = """
            INSERT INTO workflow_runs (workflow_id, name, status, version, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (workflow_id) DO UPDATE
               SET status = EXCLUDED.status, version = EXCLUDED.version, updated_at = EXCLUDED.updated_at
            """;

    private final JdbcTemplate jdbc;
    private final EventCodec codec;

    public EventStore(JdbcTemplate jdbc, EventCodec codec) {
        this.jdbc = jdbc;
        this.codec = codec;
    }

    /**
     * Appends events that the decider already applied to {@code state}.
     * Expected version = state.version() - events.size(); if another writer already used
     * that sequence number, the primary key rejects us and the whole transaction rolls back.
     */
    @Transactional
    public void append(WorkflowState state, List<WorkflowEvent> events) {
        if (events.isEmpty()) return;
        String id = state.workflowId();
        long expectedVersion = state.version() - events.size();

        List<Object[]> rows = new ArrayList<>(events.size());
        long sequence = expectedVersion;
        for (WorkflowEvent e : events) {
            rows.add(new Object[]{id, ++sequence, e.type(), codec.encode(e), Timestamp.from(e.occurredAt())});
        }
        try {
            jdbc.batchUpdate(INSERT_EVENT, rows);
        } catch (DuplicateKeyException ex) {
            throw new ConcurrencyConflictException(id, expectedVersion, ex);
        }

        jdbc.update(UPSERT_RUN, id, state.definition().name(), state.status().name(), state.version(),
                Timestamp.from(state.startedAt()), Timestamp.from(events.getLast().occurredAt()));
    }

    public List<WorkflowEvent> load(String workflowId) {
        return jdbc.query(
                "SELECT event_type, payload::text FROM workflow_events WHERE workflow_id = ? ORDER BY sequence",
                (rs, row) -> codec.decode(rs.getString(1), rs.getString(2)),
                workflowId);
    }

    public List<String> findRunningWorkflowIds() {
        return jdbc.queryForList("SELECT workflow_id FROM workflow_runs WHERE status = 'RUNNING'", String.class);
    }
}
