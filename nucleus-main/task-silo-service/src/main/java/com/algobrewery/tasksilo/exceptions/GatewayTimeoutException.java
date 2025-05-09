package com.algobrewery.tasksilo.exceptions;

import lombok.NonNull;

public class GatewayTimeoutException extends ServiceException {

    public GatewayTimeoutException() {}

    public GatewayTimeoutException(String message) { super(message); }

    public GatewayTimeoutException(@NonNull String message, Throwable cause) { super(message, cause); }

    public GatewayTimeoutException(Throwable cause) { super(cause); }

}
