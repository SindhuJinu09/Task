package com.algobrewery.tasksilo.model.internal;

import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Data
public abstract class BaseInternalRequest {
    @NotNull
    protected InternalRequestContext requestContext;
}
