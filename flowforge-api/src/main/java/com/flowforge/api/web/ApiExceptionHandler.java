package com.flowforge.api.web;

import com.flowforge.api.service.WorkflowNotFoundException;
import com.flowforge.api.service.WorkflowService.CommandPublishException;
import com.flowforge.common.dag.InvalidWorkflowException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** RFC 9457 problem details for every error the API returns. */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(InvalidWorkflowException.class)
    ProblemDetail invalidWorkflow(InvalidWorkflowException e) {
        ProblemDetail p = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Workflow definition is invalid");
        p.setProperty("errors", e.errors());
        return p;
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ProblemDetail unreadable(HttpMessageNotReadableException e) {
        Throwable root = e;
        while (root.getCause() != null) root = root.getCause();
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, root.getMessage());
    }

    @ExceptionHandler(WorkflowNotFoundException.class)
    ProblemDetail notFound(WorkflowNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(CommandPublishException.class)
    ProblemDetail unavailable(CommandPublishException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
    }
}
