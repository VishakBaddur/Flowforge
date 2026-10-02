package com.flowforge.worker.runtime;

import com.flowforge.common.messaging.TaskCommand;
import com.flowforge.worker.handler.TaskHandler;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Runs a handler on a virtual thread and enforces the task's timeout by interrupting it. */
@Component
public class TaskRunner {

    private final Map<String, TaskHandler> handlers;
    private final ExecutorService virtualThreads = Executors.newVirtualThreadPerTaskExecutor();

    public TaskRunner(List<TaskHandler> handlers) {
        this.handlers = handlers.stream().collect(Collectors.toUnmodifiableMap(TaskHandler::type, Function.identity()));
    }

    public Map<String, String> run(TaskCommand command) throws Exception {
        TaskHandler handler = handlers.get(command.type());
        if (handler == null) throw new IllegalArgumentException("no handler for task type '" + command.type() + "'");

        Future<Map<String, String>> future = virtualThreads.submit(() -> handler.execute(command));
        try {
            Map<String, String> output = future.get(command.timeoutMillis(), TimeUnit.MILLISECONDS);
            return output == null ? Map.of() : output;
        } catch (TimeoutException e) {
            future.cancel(true);   // interrupts the handler's virtual thread
            throw new TaskTimeoutException(command.timeoutMillis());
        } catch (ExecutionException e) {
            throw e.getCause() instanceof Exception cause ? cause : e;
        }
    }

    public java.util.Set<String> supportedTypes() {
        return handlers.keySet();
    }

    @PreDestroy
    void shutdown() {
        virtualThreads.shutdownNow();
    }
}
