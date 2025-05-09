package com.algobrewery.tasksilo.converter;

import com.algobrewery.tasksilo.model.external.CreateTaskRequest;
import com.algobrewery.tasksilo.model.internal.CreateTaskInternalRequest;
import com.algobrewery.tasksilo.model.internal.TaskAssigneeIDType;
import com.algobrewery.tasksilo.model.internal.TaskDTO;
import com.algobrewery.tasksilo.model.internal.TaskStatus;
import com.algobrewery.tasksilo.model.internal.TaskUpdateActorType;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

@Component("CreateTaskRequestConverter")
public class CreateTaskRequestConverter extends ExternalToInternalRequestConverter<CreateTaskRequest, CreateTaskInternalRequest> {

    @Override
    public CreateTaskInternalRequest buildRequestPayload(
            String orgUUID,
            String userUUID,
            String clientUserSessionUUID,
            String traceID,
            String regionID,
            CreateTaskRequest external) {
        LocalDateTime current = LocalDateTime.ofInstant(Instant.now(), ZoneId.systemDefault());
        return CreateTaskInternalRequest.builder()
                .taskDTO(TaskDTO.builder()
                        .taskUuid(UUID.randomUUID().toString())
                        .organizationUuid(orgUUID)
                        .title(external.getTitle())
                        .description(external.getDescription())
                        .authorUuid(userUUID)
                        .assigneeUuid(external.getAssigneeInfo().getUuid())
                        .assigneeUuidType(TaskAssigneeIDType.valueOf(external.getAssigneeInfo().getIdType().name()))
                        .createdAt(current)
                        .dueAt(external.getDueAt())
                        .updatedAt(current)
                        .updateActorType(TaskUpdateActorType.USER)
                        .status(TaskStatus.READY)
                        .parentTaskUuid(external.getRequestContext().getParentTaskUuid())
                        .extensionsData(external.getExtensionsData())
                        .build())
                .build();
    }
}
