package com.algobrewery.tasksilo.controller;

import com.algobrewery.tasksilo.converter.CreateTaskRequestConverter;
import com.algobrewery.tasksilo.converter.CreateTaskResponseConverter;
import com.algobrewery.tasksilo.converter.GetTaskRequestConverter;
import com.algobrewery.tasksilo.converter.GetTaskResponseConverter;
import com.algobrewery.tasksilo.model.external.CreateTaskRequest;
import com.algobrewery.tasksilo.model.external.CreateTaskResponse;
import com.algobrewery.tasksilo.model.external.GetTaskResponse;
import com.algobrewery.tasksilo.model.internal.CreateTaskInternalRequest;
import com.algobrewery.tasksilo.model.internal.GetTaskInternalRequest;
import com.algobrewery.tasksilo.model.internal.ResponseReasonCode;
import com.algobrewery.tasksilo.model.internal.ResponseResult;
import com.algobrewery.tasksilo.service.TaskService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

import static com.algobrewery.constants.HeaderConstants.APP_CLIENT_USER_SESSION_UUID;
import static com.algobrewery.constants.HeaderConstants.APP_ORG_UUID;
import static com.algobrewery.constants.HeaderConstants.APP_REGION_ID;
import static com.algobrewery.constants.HeaderConstants.APP_TRACE_ID;
import static com.algobrewery.constants.HeaderConstants.APP_USER_UUID;

@RestController
@RequestMapping("/api/task")
@RequiredArgsConstructor
public class TaskController {

    private final CreateTaskRequestConverter createTaskRequestConverter;
    private final CreateTaskResponseConverter createTaskResponseConverter;
    private final GetTaskRequestConverter getTaskRequestConverter;
    private final GetTaskResponseConverter getTaskResponseConverter;

    private final TaskService taskService;

    @PostMapping
    public ResponseEntity<CreateTaskResponse> createTask(
            @RequestHeader(APP_ORG_UUID) String orgUUID,
            @RequestHeader(APP_USER_UUID) String userUUID,
            @RequestHeader(APP_CLIENT_USER_SESSION_UUID) String clientUserSessionUUID,
            @RequestHeader(APP_TRACE_ID) String traceID,
            @RequestHeader(APP_REGION_ID) String regionID,
            @Valid @RequestBody CreateTaskRequest createTaskRequest) {
        try {
            final CreateTaskInternalRequest internalRequest = createTaskRequestConverter.toInternal(
                    orgUUID,
                    userUUID,
                    clientUserSessionUUID,
                    traceID,
                    regionID,
                    createTaskRequest);
            return taskService.createTask(internalRequest)
                    .thenApply(createTaskResponseConverter::toExternal)
                    .thenApply(resp -> new ResponseEntity<>(resp, resp.getHttpStatus()))
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            return new ResponseEntity<>(
                    CreateTaskResponse.builder()
                            .responseResult(ResponseResult.FAILURE.name())
                            .responseReasonCode(ResponseReasonCode.INTERNAL_ERROR.name())
                            .httpStatus(HttpStatus.INTERNAL_SERVER_ERROR)
                            .build(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<GetTaskResponse> getTaskById(
            @RequestHeader(APP_ORG_UUID) String orgUUID,
            @RequestHeader(APP_USER_UUID) String userUUID,
            @RequestHeader(APP_CLIENT_USER_SESSION_UUID) String clientUserSessionUUID,
            @RequestHeader(APP_TRACE_ID) String traceID,
            @RequestHeader(APP_REGION_ID) String regionID,
            @PathVariable String taskUUID) {
        try {
            final GetTaskInternalRequest internalRequest = getTaskRequestConverter.toInternal(
                    orgUUID,
                    userUUID,
                    clientUserSessionUUID,
                    traceID,
                    regionID,
                    taskUUID);
            return taskService.getTask(internalRequest)
                    .thenApply(getTaskResponseConverter::toExternal)
                    .thenApply(resp -> new ResponseEntity<>(resp, resp.getHttpStatus()))
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            return new ResponseEntity<>(
                    GetTaskResponse.builder()
                            .responseResult(ResponseResult.FAILURE.name())
                            .responseReasonCode(ResponseReasonCode.INTERNAL_ERROR.name())
                            .httpStatus(HttpStatus.INTERNAL_SERVER_ERROR)
                            .build(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /*
    @PutMapping("/{id}")
    public ResponseEntity<TaskDTO> updateTask(@PathVariable Long id, @Valid @RequestBody TaskDTO taskDTO) {
        return ResponseEntity.ok(taskService.updateTask(id, taskDTO));
    }
    */
}
