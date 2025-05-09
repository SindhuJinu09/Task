package com.algobrewery.tasksilo.repository.task;

import com.algobrewery.tasksilo.model.entity.Task;
import org.springframework.data.jpa.domain.Specification;

public class TaskSpecifications {

    public static Specification<Task> withTaskUuid(String taskUuid) {
        return (root, query, cb) -> {
            if (taskUuid == null) {
                return null;
            }
            return cb.equal(root.get("task_uuid"), taskUuid);
        };
    }

    public static Specification<Task> withOrganizationUuid(String organizationUuid) {
        return (root, query, cb) -> {
            if (organizationUuid == null) {
                return null;
            }
            return cb.equal(root.get("org_uuid"), organizationUuid);
        };
    }
}
