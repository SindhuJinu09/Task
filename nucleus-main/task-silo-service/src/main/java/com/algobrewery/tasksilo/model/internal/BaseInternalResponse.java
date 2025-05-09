package com.algobrewery.tasksilo.model.internal;

import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Data
public abstract class BaseInternalResponse {
    @NotNull
    protected ResponseResult responseResult;

    @NotNull
    protected ResponseReasonCode responseReasonCode;
}
