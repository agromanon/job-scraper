-- Create the missing worker_execution_history table
CREATE TABLE worker_execution_history (
    id SERIAL PRIMARY KEY,
    worker_id INTEGER REFERENCES scraping_workers(id),
    database_id INTEGER REFERENCES scraping_databases(id),
    execution_start TIMESTAMP NOT NULL,
    execution_end TIMESTAMP,
    status VARCHAR(20) NOT NULL,  -- success, failed, timeout
    
    -- Execution metrics
    jobs_found INTEGER DEFAULT 0,
    jobs_inserted INTEGER DEFAULT 0,
    jobs_updated INTEGER DEFAULT 0,
    jobs_skipped INTEGER DEFAULT 0,
    duplicates_found INTEGER DEFAULT 0,
    
    -- Performance metrics
    duration_seconds INTEGER,
    memory_used_mb INTEGER,
    cpu_usage_percent DECIMAL(5,2),
    network_requests INTEGER,
    network_errors INTEGER,
    proxy_errors INTEGER,
    
    -- Error handling
    error_message TEXT,
    error_stacktrace TEXT,
    retry_count INTEGER DEFAULT 0,
    
    -- Database performance
    db_connection_time_ms INTEGER,
    db_query_time_ms INTEGER,
    db_records_per_second DECIMAL(10,2),
    
    -- Metadata
    execution_id UUID UNIQUE NOT NULL DEFAULT gen_random_uuid(),
    worker_version VARCHAR(20),
    hostname VARCHAR(255),
    ip_address INET,
    user_agent TEXT,
    
    CONSTRAINT history_status_check CHECK (status IN ('success', 'failed', 'timeout'))
);

-- Create indexes for performance
CREATE INDEX idx_worker_execution_history_worker_id ON worker_execution_history(worker_id);
CREATE INDEX idx_worker_execution_history_status ON worker_execution_history(status);
CREATE INDEX idx_worker_execution_history_timestamp ON worker_execution_history(execution_start);
CREATE INDEX idx_worker_execution_history_database_id ON worker_execution_history(database_id);