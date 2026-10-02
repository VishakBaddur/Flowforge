package com.flowforge.api.web;

import com.flowforge.api.service.WorkflowNotFoundException;
import org.springframework.security.core.Authentication;

/** Ownership rules. Hidden workflows return 404 (not 403) so ids of other users' workflows aren't confirmed. */
final class Access {

    private static final String ADMIN = "SCOPE_workflows:admin";

    private Access() {
    }

    static boolean isAdmin(Authentication auth) {
        return auth.getAuthorities().stream().anyMatch(a -> ADMIN.equals(a.getAuthority()));
    }

    static void requireVisible(String owner, String workflowId, Authentication auth) {
        if (!isAdmin(auth) && !auth.getName().equals(owner)) throw new WorkflowNotFoundException(workflowId);
    }

    /** null = no owner filter. */
    static String ownerFilter(Authentication auth) {
        return isAdmin(auth) ? null : auth.getName();
    }
}
