package com.algobrewery.tasksilo.model.external;

import lombok.Data;

@Data
public class CreateTaskRequestContext {
    private String parentTaskUuid;
}
