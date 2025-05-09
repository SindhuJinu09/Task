package com.algobrewery.tasksilo.model.external;

import com.algobrewery.tasksilo.model.internal.TaskDTO;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

@EqualsAndHashCode(callSuper = true)
@SuperBuilder
@Data
public class GetTaskResponse extends BaseResponse {
    private TaskDTO taskDTO;
}
