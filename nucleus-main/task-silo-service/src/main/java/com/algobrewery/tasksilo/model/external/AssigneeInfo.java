package com.algobrewery.tasksilo.model.external;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class AssigneeInfo {

    public enum IDType {
        INTERNAL_ID,
        EXTERNAL_ID
    }

    @NotBlank
    private String uuid;

    @NotNull
    private IDType idType;
}
