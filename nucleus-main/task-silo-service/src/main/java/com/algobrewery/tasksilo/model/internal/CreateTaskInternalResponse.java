package com.algobrewery.tasksilo.model.internal;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

@EqualsAndHashCode(callSuper = true)
@SuperBuilder
@Data
public class CreateTaskInternalResponse extends BaseInternalResponse {
    private String taskUuid;
}
