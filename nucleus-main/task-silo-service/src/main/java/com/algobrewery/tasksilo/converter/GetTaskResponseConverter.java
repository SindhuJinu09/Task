package com.algobrewery.tasksilo.converter;

import com.algobrewery.tasksilo.model.external.GetTaskResponse;
import com.algobrewery.tasksilo.model.internal.GetTaskInternalResponse;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component("GetTaskResponseConverter")
public class GetTaskResponseConverter extends InternalToExternalResponseConverter<GetTaskInternalResponse, GetTaskResponse> {

    @Override
    protected HttpStatus getSuccessHttpStatusCode() {
        return HttpStatus.OK;
    }

    @Override
    protected GetTaskResponse buildResponsePayload(GetTaskInternalResponse internal) {
        return GetTaskResponse.builder().taskDTO(internal.getTaskDTO()).build();
    }
}
