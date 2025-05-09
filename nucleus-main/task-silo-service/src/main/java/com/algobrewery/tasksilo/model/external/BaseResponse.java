package com.algobrewery.tasksilo.model.external;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.experimental.SuperBuilder;
import org.springframework.http.HttpStatus;

@SuperBuilder
@Data
public abstract class BaseResponse {

    @NotNull
    protected HttpStatus httpStatus;

    @NotBlank
    protected String responseResult;

    @NotBlank
    protected String responseReasonCode;
}
