package com.algobrewery.tasksilo.converter;

import com.algobrewery.tasksilo.model.internal.BaseInternalRequest;
import com.algobrewery.tasksilo.utils.InternalRequestContextConverter;

public abstract class ExternalToInternalRequestConverter<I, O extends BaseInternalRequest> {

    public O toInternal(
            String orgUUID,
            String userUUID,
            String clientUserSessionUUID,
            String traceID,
            String regionID,
            I external) {
        O internal = buildRequestPayload(
                orgUUID,
                userUUID,
                clientUserSessionUUID,
                traceID,
                regionID,
                external);
        internal.setRequestContext(
                InternalRequestContextConverter.fromHeaders(
                        orgUUID,
                        userUUID,
                        clientUserSessionUUID,
                        traceID,
                        regionID));
        return internal;
    }

    protected abstract O buildRequestPayload(
            String orgUUID,
            String userUUID,
            String clientUserSessionUUID,
            String traceID,
            String regionID,
            I external);
}
