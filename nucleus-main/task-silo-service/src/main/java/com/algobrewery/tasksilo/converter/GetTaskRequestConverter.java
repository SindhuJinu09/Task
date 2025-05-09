package com.algobrewery.tasksilo.converter;

import com.algobrewery.tasksilo.model.internal.GetTaskInternalRequest;
import org.springframework.stereotype.Component;

@Component("GetTaskRequestConverter")
public class GetTaskRequestConverter extends ExternalToInternalRequestConverter<String, GetTaskInternalRequest> {
    @Override
    protected GetTaskInternalRequest buildRequestPayload(
            String orgUUID,
            String userUUID,
            String clientUserSessionUUID,
            String traceID,
            String regionID,
            String external) {
        return GetTaskInternalRequest.builder().taskUuid(external).build();
    }
}
