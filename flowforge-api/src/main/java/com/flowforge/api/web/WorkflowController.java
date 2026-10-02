package com.flowforge.api.web;

import com.flowforge.api.service.WorkflowService;
import com.flowforge.api.web.ApiModels.EventView;
import com.flowforge.api.web.ApiModels.PageResponse;
import com.flowforge.api.web.ApiModels.SubmitResponse;
import com.flowforge.api.web.ApiModels.WorkflowSummary;
import com.flowforge.api.web.ApiModels.WorkflowView;
import com.flowforge.common.model.WorkflowDefinition;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.List;

@RestController
@RequestMapping("/api/v1/workflows")
public class WorkflowController {

    private final WorkflowService service;

    public WorkflowController(WorkflowService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<SubmitResponse> submit(@RequestBody WorkflowDefinition definition,
                                                 @RequestHeader(name = "Idempotency-Key", required = false) String idempotencyKey) {
        String id = service.submit(definition, idempotencyKey);
        return ResponseEntity.accepted()
                .location(URI.create("/api/v1/workflows/" + id))
                .body(new SubmitResponse(id, "ACCEPTED"));
    }

    @GetMapping("/{workflowId}")
    public WorkflowView get(@PathVariable String workflowId) {
        return service.get(workflowId);
    }

    @GetMapping
    public PageResponse<WorkflowSummary> list(@RequestParam(required = false) String status,
                                              @PageableDefault(size = 20, sort = "createdAt", direction = Sort.Direction.DESC)
                                              Pageable pageable) {
        return service.list(status, pageable);
    }

    @GetMapping("/{workflowId}/events")
    public List<EventView> events(@PathVariable String workflowId) {
        return service.events(workflowId);
    }

    @PostMapping("/{workflowId}/cancel")
    public ResponseEntity<SubmitResponse> cancel(@PathVariable String workflowId,
                                                 @RequestParam(defaultValue = "cancelled via API") String reason) {
        service.cancel(workflowId, reason);
        return ResponseEntity.accepted().body(new SubmitResponse(workflowId, "CANCEL_REQUESTED"));
    }
}
