package com.algobrewery.tasksilo.utils;

import com.algobrewery.tasksilo.model.internal.InternalRequestContext;

public final class InternalRequestContextConverter {

    public static InternalRequestContext fromHeaders(
            String orgUUID,
            String userUUID,
            String clientUserSessionUUID,
            String traceID,
            String regionID) {
        return InternalRequestContext.builder()
                .appOrgUuid(orgUUID)
                .appUserUuid(userUUID)
                .appClientUserSessionUuid(clientUserSessionUUID)
                .traceId(traceID)
                .regionId(regionID)
                .build();
    }

}
