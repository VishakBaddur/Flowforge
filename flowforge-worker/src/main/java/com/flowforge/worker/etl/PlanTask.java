package com.flowforge.worker.etl;

import com.flowforge.common.messaging.TaskCommand;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Map;

@Component
class PlanTask extends EtlTask {

    PlanTask(JdbcTemplate jdbc, PlatformTransactionManager txManager) {
        super(jdbc, txManager);
    }

    @Override
    public String type() {
        return "etl.plan";
    }

    @Override
    public Map<String, String> execute(TaskCommand c) {
        String runId = in(c, "run_id");
        jdbc.update("""
                INSERT INTO etl.run (run_id, pipeline, mode, start_date, end_date, partitions, status, started_at)
                VALUES (?, ?, ?, ?::date, ?::date, ?, 'RUNNING', now())
                ON CONFLICT (run_id) DO NOTHING
                """, runId, in(c, "pipeline", DEFAULT_PIPELINE), in(c, "mode", "backfill"),
                in(c, "start_date"), in(c, "end_date"), Integer.parseInt(in(c, "partitions")));
        return Map.of("run_id", runId);
    }
}
