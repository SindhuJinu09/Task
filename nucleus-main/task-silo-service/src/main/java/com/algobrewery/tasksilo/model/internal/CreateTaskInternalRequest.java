package com.algobrewery.tasksilo.model.internal;

import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

@EqualsAndHashCode(callSuper = true)
@SuperBuilder
@Data
public class CreateTaskInternalRequest extends BaseInternalRequest {
    @NotNull
    private TaskDTO taskDTO;
}
