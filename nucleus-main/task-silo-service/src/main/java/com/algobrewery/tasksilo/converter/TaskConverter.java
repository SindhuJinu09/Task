package com.algobrewery.tasksilo.converter;

import com.algobrewery.converters.BiDirectionalConverter;
import com.algobrewery.tasksilo.model.entity.Task;
import com.algobrewery.tasksilo.model.internal.TaskAssigneeIDType;
import com.algobrewery.tasksilo.model.internal.TaskDTO;
import com.algobrewery.tasksilo.model.internal.TaskStatus;
import com.algobrewery.tasksilo.model.internal.TaskUpdateActorType;
import jakarta.validation.constraints.NotNull;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component("TaskConverter")
public class TaskConverter implements BiDirectionalConverter<TaskDTO, Task> {

    @Override
    public Task doForward(@NotNull TaskDTO input) {
        return Task.builder()
                .taskUuid(input.getTaskUuid())
                .organizationUuid(input.getOrganizationUuid())
                .title(input.getTitle())
                .description(input.getDescription())
                .authorUuid(input.getAuthorUuid())
                .assigneeUuidType(input.getAssigneeUuidType().name())
                .createdAt(input.getCreatedAt())
                .dueAt(input.getDueAt())
                .updatedAt(input.getUpdatedAt())
                .updateActorType(input.getUpdateActorType().name())
                .status(input.getStatus().name())
                .parentTaskUuid(input.getParentTaskUuid())
                .childTaskUuids(input.getChildTaskUuids().toArray(new String[0]))
                .extensionsData(input.getExtensionsData())
                .build();
    }

    @Override
    public TaskDTO doBackward(Task input) {
        return TaskDTO.builder()
                .taskUuid(input.getTaskUuid())
                .organizationUuid(input.getOrganizationUuid())
                .title(input.getTitle())
                .description(input.getDescription())
                .authorUuid(input.getAuthorUuid())
                .assigneeUuidType(TaskAssigneeIDType.valueOf(input.getAssigneeUuidType()))
                .createdAt(input.getCreatedAt())
                .dueAt(input.getDueAt())
                .updatedAt(input.getUpdatedAt())
                .updateActorType(TaskUpdateActorType.valueOf(input.getUpdateActorType()))
                .status(TaskStatus.valueOf(input.getStatus()))
                .parentTaskUuid(input.getParentTaskUuid())
                .childTaskUuids(Arrays.asList(input.getChildTaskUuids()))
                .extensionsData(input.getExtensionsData())
                .build();
    }
}
