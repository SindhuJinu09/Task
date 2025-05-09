package com.algobrewery.tasksilo.repository.task;

import com.algobrewery.tasksilo.model.entity.Task;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface TaskRepository extends JpaRepository<Task, String>, JpaSpecificationExecutor<Task> {

    @Query("SELECT t FROM Task t WHERE t.taskUuid = :taskUuid AND t.organizationUuid = :organizationUuid")
    Optional<Task> findByTaskUuid(
            @Param("taskUuid") String taskUuid,
            @Param("organizationUuid") String organizationUuid);

    @Query("SELECT t FROM Task t WHERE t.authorUuid = :authorUuid AND t.organizationUuid = :organizationUuid")
    List<Task> findByAuthorUuid(
            @Param("authorUuid") String authorUuid,
            @Param("organizationUuid") String organizationUuid);

    @Query("SELECT t FROM Task t WHERE t.assigneeUuid = :assigneeUuid AND t.organizationUuid = :organizationUuid")
    List<Task> findByAssigneeUuid(
            @Param("assigneeUuid") String assigneeUuid,
            @Param("organizationUuid") String organizationUuid);

    @Query("SELECT t FROM Task t WHERE t.status = :status AND t.organizationUuid = :organizationUuid")
    List<Task> findByStatus(
            @Param("status") String status,
            @Param("organizationUuid") String organizationUuid);

    @Query("SELECT t FROM Task t WHERE t.parentTaskUuid = :parentTaskUuid AND t.organizationUuid = :organizationUuid")
    List<Task> findByParentTaskUuid(
            @Param("parentTaskUuid") String parentTaskUuid,
            @Param("organizationUuid") String organizationUuid);
}
