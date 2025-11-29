-- init.sql: creates schema and hypertable for operations (ops)
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE topic_user (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Tenant info (optional but common in SaaS)
    tenant_id TEXT UNIQUE NOT NULL,
    public_key_pem TEXT NOT NULL,

    -- Metadata
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO topic_user (tenant_id, public_key_pem)
VALUES (
    'ADMIN',
    '-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCcwi14gdnPC99h6xoF3xGRh0U6
XcEJr07/i+/NGzR4UIHg+Pn2+Dkx2z4ys2HQ+OhJGL4p6AIEm7PwJoGH3SY75H7N
EVjjEYCQHf6PvdzQyep+VDtvjmWefo+RjafpHfpA+Wm7B7KFqnC1+3Iy4R05sQul
V5vuDYnEmv5xWAwBTwIDAQAB
-----END PUBLIC KEY-----'
)
RETURNING *;

CREATE TABLE topic_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic_name TEXT NOT NULL,
    description TEXT,
    environment TEXT CHECK (environment IN ('dev', 'staging', 'prod')),
    user_id UUID NOT NULL REFERENCES topic_user(id) ON DELETE CASCADE,

    partitions INT NOT NULL DEFAULT 1,
    replication_factor INT NOT NULL,
    retention_hours INT DEFAULT 168, -- 7 days
    cleanup_policy TEXT CHECK (cleanup_policy IN ('delete', 'compact')),

    type TEXT CHECK (type IN ('classic', 'distinct', 'priority')),
    schema_type TEXT CHECK (schema_type IN ('avro', 'json', 'protobuf')),
    schema_definition JSONB,  -- You can store schema or a reference

    owner_team TEXT,
    tags TEXT[],

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uniq_user_topic UNIQUE (user_id, topic_name)
);

CREATE INDEX ON topic_metadata (user_id);
CREATE INDEX ON topic_metadata (topic_name);

CREATE TABLE deleted_topic_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic_name TEXT NOT NULL,
    description TEXT,
    environment TEXT,
    user_id UUID,

    partitions INT NOT NULL,
    replication_factor INT NOT NULL,
    retention_hours INT,
    cleanup_policy TEXT,

    type TEXT,
    schema_type TEXT,
    schema_definition JSONB,

    owner_team TEXT,
    tags TEXT[],

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE consumer_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    user_id UUID NOT NULL REFERENCES topic_user(id) ON DELETE CASCADE,
    topic_id UUID NOT NULL REFERENCES topic_metadata(id) ON DELETE CASCADE,
    
    partition_id INT NOT NULL,
    partition_offset BIGINT NOT NULL DEFAULT 0,
    leader_broker TEXT,
    
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(topic_id, partition_id, name)
);


