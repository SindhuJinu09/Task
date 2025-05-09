package com.algobrewery.tasksilo.exceptions;

import lombok.NonNull;

public class ServiceException extends Exception {

    public ServiceException() {}

    public ServiceException(String message) { super(message); }

    public ServiceException(@NonNull String message, Throwable cause) { super(message, cause); }

    public ServiceException(Throwable cause) { super(cause); }
}
