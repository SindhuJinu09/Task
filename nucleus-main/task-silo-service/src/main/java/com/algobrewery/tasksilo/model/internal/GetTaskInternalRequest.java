package com.algobrewery.tasksilo.model.internal;

import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

@EqualsAndHashCode(callSuper = true)
@SuperBuilder
@Data
public class GetTaskInternalRequest extends BaseInternalRequest {
    @NotNull
    private String taskUuid;
}
