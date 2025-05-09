package com.algobrewery.tasksilo.service.impl;

import com.algobrewery.tasksilo.converter.TaskConverter;
import com.algobrewery.tasksilo.exceptions.GatewayTimeoutException;
import com.algobrewery.tasksilo.exceptions.InvalidRequestError;
import com.algobrewery.tasksilo.exceptions.NotFoundException;
import com.algobrewery.tasksilo.exceptions.ServiceException;
import com.algobrewery.tasksilo.gateway.UserServiceGateway;
import com.algobrewery.tasksilo.model.entity.Task;
import com.algobrewery.tasksilo.model.internal.CreateTaskInternalRequest;
import com.algobrewery.tasksilo.model.internal.CreateTaskInternalResponse;
import com.algobrewery.tasksilo.model.internal.GetTaskInternalRequest;
import com.algobrewery.tasksilo.model.internal.GetTaskInternalResponse;
import com.algobrewery.tasksilo.model.internal.ResponseReasonCode;
import com.algobrewery.tasksilo.model.internal.ResponseResult;
import com.algobrewery.tasksilo.model.internal.TaskDTO;
import com.algobrewery.tasksilo.repository.task.TaskRepository;
import com.algobrewery.tasksilo.service.TaskService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.algobrewery.tasksilo.repository.task.TaskSpecifications.withOrganizationUuid;
import static com.algobrewery.tasksilo.repository.task.TaskSpecifications.withTaskUuid;

@Service
@RequiredArgsConstructor
public class TaskServiceImpl implements TaskService {

    private final UserServiceGateway userServiceGateway;
    private final TaskConverter taskConverter;
    private final TaskRepository taskRepository;

    @Override
    public CompletableFuture<CreateTaskInternalResponse> createTask(CreateTaskInternalRequest request) {
        // Skip user validation and directly save the task
        return saveTask(request.getTaskDTO())
                .thenCompose(this::buildCreateTaskInternalResponse)
                .exceptionally(this::handleCreateTaskException);
    }

    private CompletableFuture<Task> saveTask(TaskDTO taskDTO) {
        try {
            return CompletableFuture.completedFuture(taskRepository.save(taskConverter.doForward(taskDTO)));
        } catch (Exception e) {
            return CompletableFuture.failedFuture(new ServiceException(e.getMessage(), e.getCause()));
        }
    }

    private CompletableFuture<CreateTaskInternalResponse> buildCreateTaskInternalResponse(Task task) {
        return CompletableFuture.completedFuture(CreateTaskInternalResponse.builder()
                .taskUuid(task.getTaskUuid())
                .responseResult(ResponseResult.SUCCESS)
                .responseReasonCode(ResponseReasonCode.SUCCESS)
                .build());
    }

    private CreateTaskInternalResponse handleCreateTaskException(Throwable ex) {
        Throwable cause = ex;
        if (!Objects.isNull(ex) && ex instanceof CompletionException) {
            cause = ex.getCause();
        }
        if (cause instanceof InvalidRequestError) {
            return CreateTaskInternalResponse.builder()
                    .responseResult(ResponseResult.FAILURE)
                    .responseReasonCode(ResponseReasonCode.BAD_REQUEST)
                    .build();
        }
        if (cause instanceof NotFoundException) {
            return CreateTaskInternalResponse.builder()
                    .responseResult(ResponseResult.FAILURE)
                    .responseReasonCode(ResponseReasonCode.BAD_REQUEST)
                    .build();
        }
        if (cause instanceof GatewayTimeoutException) {
            return CreateTaskInternalResponse.builder()
                    .responseResult(ResponseResult.FAILURE)
                    .responseReasonCode(ResponseReasonCode.TIMEOUT)
                    .build();
        }
        if (cause instanceof ServiceException) {
            return CreateTaskInternalResponse.builder()
                    .responseResult(ResponseResult.FAILURE)
                    .responseReasonCode(ResponseReasonCode.INTERNAL_ERROR)
                    .build();
        }
        throw new CompletionException(new ServiceException(cause));
    }

    public CompletableFuture<GetTaskInternalResponse> getTask(GetTaskInternalRequest request) {
        return CompletableFuture.completedFuture(
                        Specification.where(withTaskUuid(request.getTaskUuid()))
                                .and(withOrganizationUuid(request.getRequestContext().getAppOrgUuid())))
                .thenCompose(this::getTask)
                .thenCompose(this::buildGetTaskInternalResponse)
                .exceptionally(this::handleGetTaskException);
    }

    private CompletableFuture<Task> getTask(Specification<Task> spec) {
        try {
            return taskRepository.findOne(spec)
                    .map(CompletableFuture::completedFuture)
                    .orElseGet(() -> CompletableFuture.failedFuture(new NotFoundException()));
        } catch (Exception e) {
            return CompletableFuture.failedFuture(new ServiceException(e.getMessage(), e.getCause()));
        }
    }

    private CompletableFuture<GetTaskInternalResponse> buildGetTaskInternalResponse(Task task) {
        return CompletableFuture.completedFuture(GetTaskInternalResponse.builder()
                .taskDTO(taskConverter.doBackward(task))
                .responseResult(ResponseResult.SUCCESS)
                .responseReasonCode(ResponseReasonCode.SUCCESS)
                .build());
    }

    private GetTaskInternalResponse handleGetTaskException(Throwable ex) {
        Throwable cause = ex;
        if (!Objects.isNull(ex) && ex instanceof CompletionException) {
            cause = ex.getCause();
        }
        if (cause instanceof InvalidRequestError) {
            return GetTaskInternalResponse.builder()
                    .responseResult(ResponseResult.FAILURE)
                    .responseReasonCode(ResponseReasonCode.BAD_REQUEST)
                    .build();
        }
        if (cause instanceof NotFoundException) {
            return GetTaskInternalResponse.builder()
                    .responseResult(ResponseResult.FAILURE)
                    .responseReasonCode(ResponseReasonCode.BAD_REQUEST)
                    .build();
        }
        if (cause instanceof GatewayTimeoutException) {
            return GetTaskInternalResponse.builder()
                    .responseResult(ResponseResult.FAILURE)
                    .responseReasonCode(ResponseReasonCode.TIMEOUT)
                    .build();
        }
        if (cause instanceof ServiceException) {
            return GetTaskInternalResponse.builder()
                    .responseResult(ResponseResult.FAILURE)
                    .responseReasonCode(ResponseReasonCode.INTERNAL_ERROR)
                    .build();
        }
        throw new CompletionException(new ServiceException(cause));
    }
}