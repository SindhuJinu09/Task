package com.algobrewery.tasksilo.gateway;

import com.algobrewery.tasksilo.model.internal.InternalRequestContext;

import java.util.concurrent.CompletableFuture;

public interface UserServiceGateway {
    CompletableFuture<String> getUser(InternalRequestContext requestContext, String userUuid);
}
