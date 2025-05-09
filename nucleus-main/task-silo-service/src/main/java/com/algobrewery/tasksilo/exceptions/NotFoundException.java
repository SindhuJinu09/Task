package com.algobrewery.tasksilo.exceptions;

import lombok.NonNull;

public class NotFoundException extends ServiceException {

    public NotFoundException() {}

    public NotFoundException(String message) { super(message); }

    public NotFoundException(@NonNull String message, Throwable cause) { super(message, cause); }

    public NotFoundException(Throwable cause) { super(cause); }

}
