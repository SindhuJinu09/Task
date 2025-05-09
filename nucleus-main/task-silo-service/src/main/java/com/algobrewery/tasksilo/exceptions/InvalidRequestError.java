package com.algobrewery.tasksilo.exceptions;

import lombok.NonNull;

public class InvalidRequestError extends ServiceException {

    public InvalidRequestError() {}

    public InvalidRequestError(String message) { super(message); }

    public InvalidRequestError(@NonNull String message, Throwable cause) { super(message, cause); }

    public InvalidRequestError(Throwable cause) { super(cause); }

}
