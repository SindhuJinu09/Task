package com.algobrewery.tasksilo.model.gateway.userservice;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;

@Data
public class GetUserResponse {
    private String userId;
    private String username;
    private String firstName;
    private String middleName;
    private String lastName;
    private String email;
    private String phone;
    private LocalDateTime startDate;
    private LocalDateTime endDate;
    private String status;
    private JobProfileInfo currentJobProfile;
    private List<JobProfileInfo> previousJobProfiles;
}
