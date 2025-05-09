package com.algobrewery.tasksilo.model.entity;

import com.algobrewery.persistence.attributeconverters.StringArrayConverter;
import com.algobrewery.tasksilo.model.entity.attributeconverter.TaskExtensionsDataConverter;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;

@Entity
@Table(name = "tasks")
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Task {

    @Id
    @Column(name = "task_uuid")
    private String taskUuid;

    @Column(name = "org_uuid", nullable = false)
    private String organizationUuid;

    @Column(name = "title", nullable = false)
    private String title;

    @Column(name = "description")
    private String description;

    @Column(name = "author_uuid", nullable = false)
    private String authorUuid;

    @Column(name = "assignee_uuid", nullable = false)
    private String assigneeUuid;

    @Column(name = "assignee_uuid_type", nullable = false)
    private String assigneeUuidType;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "due_at", nullable = false)
    private LocalDateTime dueAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @Column(name = "update_actor_type")
    private String updateActorType;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "parent_task_uuid")
    private String parentTaskUuid;

    @Column(columnDefinition = "text[]", name = "child_task_uuids")
    @Convert(converter = StringArrayConverter.class)
    private String[] childTaskUuids;

    @Column(columnDefinition = "string", name = "extensions_data")
    @Convert(converter = TaskExtensionsDataConverter.class)
    private Map<String, Object> extensionsData;
}
