package com.algobrewery.tasksilo.model.internal;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

@EqualsAndHashCode(callSuper = true)
@SuperBuilder
@Data
public class GetTaskInternalResponse extends BaseInternalResponse {
    private TaskDTO taskDTO;
}
