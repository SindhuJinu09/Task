package com.algobrewery.tasksilo.converter;

import com.algobrewery.tasksilo.model.external.BaseResponse;
import com.algobrewery.tasksilo.model.internal.BaseInternalResponse;
import com.algobrewery.tasksilo.model.internal.ResponseReasonCode;
import org.springframework.http.HttpStatus;

public abstract class InternalToExternalResponseConverter<I extends BaseInternalResponse, O extends BaseResponse> {

    public O toExternal(I internal) {
        O external = buildResponsePayload(internal);
        external.setHttpStatus(mapHttpStatus(internal.getResponseReasonCode()));
        external.setResponseResult(internal.getResponseResult().name());
        external.setResponseReasonCode(internal.getResponseReasonCode().name());
        return external;
    }

    protected HttpStatus mapHttpStatus(ResponseReasonCode responseReasonCode) {
        return switch (responseReasonCode) {
            case INTERNAL_ERROR -> HttpStatus.INTERNAL_SERVER_ERROR;
            case EXTERNAL_DEPENDENCY_FAILURE -> HttpStatus.FAILED_DEPENDENCY;
            case TIMEOUT -> HttpStatus.REQUEST_TIMEOUT;
            case BAD_REQUEST -> HttpStatus.BAD_REQUEST;
            default -> getSuccessHttpStatusCode();
        };
    }

    protected abstract HttpStatus getSuccessHttpStatusCode();

    protected abstract O buildResponsePayload(I internal);

}
