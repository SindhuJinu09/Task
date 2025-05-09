package com.algobrewery.tasksilo.model.external;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.Map;

@Data
public class CreateTaskRequest {

    @NotNull
    private CreateTaskRequestContext requestContext;

    @NotBlank
    private String title;

    private String description;

    @NotNull
    private AssigneeInfo assigneeInfo;

    @NotNull
    private LocalDateTime dueAt;

    @NotNull
    private Map<String, Object> extensionsData;
}
