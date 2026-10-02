package com.flowforge.orchestrator.store;

import com.flowforge.common.event.EventCodec;
import com.flowforge.common.event.WorkflowEvent;
import com.flowforge.common.state.WorkflowState;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Repository
public class EventStore {

    private static final String INSERT_EVENT = """
            INSERT INTO workflow_events (workflow_id, sequence, event_type, payload, created_at)
            VALUES (?, ?, ?, ?::jsonb, ?)
            """;

    private static final String UPSERT_RUN = """
            INSERT INTO workflow_runs (workflow_id, name, owner, status, version, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
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

        jdbc.update(UPSERT_RUN, id, state.definition().name(), state.owner(), state.status().name(), state.version(),
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

    /** Loads many workflows in ONE query. Used by crash recovery so it scales with data, not round trips. */
    public Map<String, List<WorkflowEvent>> loadAll(Collection<String> workflowIds) {
        Map<String, List<WorkflowEvent>> result = new LinkedHashMap<>();
        if (workflowIds.isEmpty()) return result;
        jdbc.query(con -> {
                    PreparedStatement ps = con.prepareStatement("""
                            SELECT workflow_id, event_type, payload::text FROM workflow_events
                            WHERE workflow_id = ANY(?) ORDER BY workflow_id, sequence
                            """);
                    ps.setArray(1, con.createArrayOf("varchar", workflowIds.toArray()));
                    return ps;
                },
                rs -> {
                    result.computeIfAbsent(rs.getString(1), k -> new ArrayList<>())
                            .add(codec.decode(rs.getString(2), rs.getString(3)));
                });
        return result;
    }
}
