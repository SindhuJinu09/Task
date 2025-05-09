package com.algobrewery.tasksilo.model.gateway.userservice;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
public class JobProfileInfo {

    private String jobTitle;
    private LocalDateTime startDate;
    private LocalDateTime endDate;
    private String reportingManager;
    private List<String> reportees;
    private String organizationUnit;
    private Map<String, Object> extensionsData;

}
