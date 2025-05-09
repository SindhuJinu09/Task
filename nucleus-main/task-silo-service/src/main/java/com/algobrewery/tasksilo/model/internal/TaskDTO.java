package com.algobrewery.tasksilo.model.internal;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
@Builder
public class TaskDTO {

    @NotBlank
    private String taskUuid;

    @NotBlank
    private String organizationUuid;

    @NotBlank
    private String title;

    private String description;

    @NotBlank
    private String authorUuid;

    @NotBlank
    private String assigneeUuid;

    @NotBlank
    private TaskAssigneeIDType assigneeUuidType;

    @NotNull
    private LocalDateTime createdAt;

    @NotNull
    private LocalDateTime dueAt;

    @NotNull
    private LocalDateTime updatedAt;

    @NotNull
    private TaskUpdateActorType updateActorType;

    @NotNull
    private TaskStatus status;

    private String parentTaskUuid;

    @NotNull
    private List<String> childTaskUuids;

    @NotNull
    private Map<String, Object> extensionsData;
}
