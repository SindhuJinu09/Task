package com.algobrewery.tasksilo.model.internal;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class InternalRequestContext {
    private String appOrgUuid;
    private String appUserUuid;
    private String appClientUserSessionUuid;
    private String traceId;
    private String regionId;
}
