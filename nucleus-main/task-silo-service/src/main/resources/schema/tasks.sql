CREATE TABLE IF NOT EXISTS tasks (
    task_uuid VARCHAR(36) PRIMARY KEY,
    org_uuid VARCHAR(36) NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    author_uuid VARCHAR(36) NOT NULL,
    assignee_uuid VARCHAR(36) NOT NULL,
    assignee_uuid_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    due_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP,
    update_actor_type VARCHAR(50),
    status VARCHAR(50) NOT NULL,
    parent_task_uuid VARCHAR(36),
    child_task_uuids TEXT[], -- PostgreSQL array type
    extensions_data JSONB,   -- PostgreSQL JSONB type for storing extension data

    -- Foreign key constraint for parent-child relationship
    CONSTRAINT fk_parent_task FOREIGN KEY (parent_task_uuid)
        REFERENCES tasks(task_uuid) ON DELETE SET NULL
);

-- Combined indexes for common query patterns
CREATE INDEX idx_tasks_org_status ON tasks(org_uuid, status);
CREATE INDEX idx_tasks_org_assignee_status ON tasks(org_uuid, assignee_uuid, status);
CREATE INDEX idx_tasks_org_author_status ON tasks(org_uuid, author_uuid, status);
CREATE INDEX idx_tasks_org_parent_task ON tasks(org_uuid, parent_task_uuid);