package com.algobrewery.tasksilo.converter;

import com.algobrewery.tasksilo.model.external.CreateTaskResponse;
import com.algobrewery.tasksilo.model.internal.CreateTaskInternalResponse;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component("CreateTaskResponseConverter")
public class CreateTaskResponseConverter extends InternalToExternalResponseConverter<CreateTaskInternalResponse, CreateTaskResponse> {

    @Override
    protected HttpStatus getSuccessHttpStatusCode() {
        return HttpStatus.CREATED;
    }

    @Override
    protected CreateTaskResponse buildResponsePayload(CreateTaskInternalResponse internal) {
        return CreateTaskResponse.builder()
                .taskUuid(internal.getTaskUuid())
                .build();
    }
}
