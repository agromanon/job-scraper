-- Enhanced modular scraping architecture with multi-database support v2

-- Worker configurations table
CREATE TABLE scraping_workers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    site VARCHAR(20) NOT NULL,  -- linkedin, indeed, glassdoor, google, zip_recruiter
    search_term VARCHAR(255),
    location VARCHAR(255),
    country VARCHAR(50) DEFAULT 'BRAZIL',
    distance INTEGER DEFAULT 25,
    job_type JSONB,  -- Array of job types
    is_remote BOOLEAN DEFAULT false,
    easy_apply BOOLEAN DEFAULT false,
    linkedin_company_ids INTEGER[],
    hours_old INTEGER,
    results_per_run INTEGER DEFAULT 50,
    schedule_hours INTEGER DEFAULT 24,  -- How often to run (hours)
    schedule_minute_offset INTEGER DEFAULT 0,  -- Minute offset for staggered execution
    timezone VARCHAR(50) DEFAULT 'America/Sao_Paulo',
    proxy_rotation_policy VARCHAR(20) DEFAULT 'rotating',  -- rotating, sticky, none
    proxies TEXT[],  -- Array of proxy URLs
    max_retries INTEGER DEFAULT 3,
    timeout INTEGER DEFAULT 30,
    rate_limit_requests INTEGER DEFAULT 10,
    rate_limit_seconds INTEGER DEFAULT 60,
    description_format VARCHAR(20) DEFAULT 'markdown',  -- html, markdown, plain
    linkedin_fetch_description BOOLEAN DEFAULT false,
    
    -- Database connection settings
    database_id INTEGER REFERENCES scraping_databases(id),
    table_name VARCHAR(100) NOT NULL,
    
    -- Status and monitoring
    status VARCHAR(20) DEFAULT 'active',  -- active, paused, stopped, error
    last_run TIMESTAMP,
    next_run TIMESTAMP,
    last_success TIMESTAMP,
    last_error TEXT,
    consecutive_errors INTEGER DEFAULT 0,
    max_consecutive_errors INTEGER DEFAULT 5,
    auto_pause_on_errors BOOLEAN DEFAULT true,
    
    -- Resource usage
    memory_limit_mb INTEGER DEFAULT 512,
    cpu_limit_cores DECIMAL(3,1) DEFAULT 0.5,
    max_runtime_minutes INTEGER DEFAULT 60,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tags TEXT[],  -- For categorization and filtering
    
    -- Indexes for performance
    CONSTRAINT worker_site_check CHECK (site IN ('linkedin', 'indeed', 'glassdoor', 'google', 'zip_recruiter', 'bayt', 'naukri', 'bdjobs')),
    CONSTRAINT worker_status_check CHECK (status IN ('active', 'paused', 'stopped', 'error')),
    CONSTRAINT worker_proxy_check CHECK (proxy_rotation_policy IN ('rotating', 'sticky', 'none')),
    CONSTRAINT worker_description_check CHECK (description_format IN ('html', 'markdown', 'plain'))
);

-- Database connections table for multi-database support
CREATE TABLE scraping_databases (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    host VARCHAR(255) NOT NULL,
    port INTEGER DEFAULT 5432,
    database_name VARCHAR(100) NOT NULL,
    username VARCHAR(100) NOT NULL,
    password TEXT NOT NULL,
    ssl_mode VARCHAR(20) DEFAULT 'require',
    connection_pool_size INTEGER DEFAULT 5,
    max_connections INTEGER DEFAULT 20,
    connection_timeout_seconds INTEGER DEFAULT 30,
    
    -- Schema configuration
    target_table_prefix VARCHAR(50) DEFAULT 'job_listings_',
    create_schema_if_not_exists BOOLEAN DEFAULT true,
    
    -- Performance settings
    batch_size INTEGER DEFAULT 100,
    deduplication_method VARCHAR(20) DEFAULT 'unique_id',  -- unique_id, composite_key, none
    deduplication_fields TEXT[] DEFAULT ARRAY['job_url'],
    
    -- Monitoring
    is_active BOOLEAN DEFAULT true,
    last_connection_test TIMESTAMP,
    connection_status VARCHAR(20) DEFAULT 'untested',  -- untested, success, failed
    connection_error TEXT,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tags TEXT[],
    
    CONSTRAINT database_ssl_check CHECK (ssl_mode IN ('require', 'prefer', 'allow', 'disable', 'verify-full')),
    CONSTRAINT deduplication_check CHECK (deduplication_method IN ('unique_id', 'composite_key', 'none'))
);

-- Worker execution history for monitoring
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

-- Worker schedule log for tracking planned vs actual executions
CREATE TABLE worker_schedule_log (
    id SERIAL PRIMARY KEY,
    worker_id INTEGER REFERENCES scraping_workers(id),
    scheduled_time TIMESTAMP NOT NULL,
    actual_start_time TIMESTAMP,
    execution_id UUID REFERENCES worker_execution_history(execution_id),
    
    -- Status tracking
    status VARCHAR(20) DEFAULT 'scheduled',  -- scheduled, running, completed, missed, skipped
    
    -- Delay tracking
    delay_seconds INTEGER,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT,
    
    CONSTRAINT schedule_status_check CHECK (status IN ('scheduled', 'running', 'completed', 'missed', 'skipped'))
);

-- Worker templates for quick setup
CREATE TABLE worker_templates (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    site VARCHAR(20) NOT NULL,
    template_config JSONB NOT NULL,  -- Default configuration for the worker
    
    -- Template metadata
    category VARCHAR(50),  -- entry-level, senior, remote, tech, etc.
    difficulty VARCHAR(20) DEFAULT 'medium',  -- easy, medium, hard
    is_popular BOOLEAN DEFAULT false,
    usage_count INTEGER DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    
    CONSTRAINT template_site_check CHECK (site IN ('linkedin', 'indeed', 'glassdoor', 'google', 'zip_recruiter', 'bayt', 'naukri', 'bdjobs')),
    CONSTRAINT template_difficulty_check CHECK (difficulty IN ('easy', 'medium', 'hard'))
);

-- Proxy management table
CREATE TABLE proxy_servers (
    id SERIAL PRIMARY KEY,
    url VARCHAR(255) NOT NULL,
    description TEXT,
    country VARCHAR(50),
    city VARCHAR(50),
    provider VARCHAR(100),
    protocol VARCHAR(20) DEFAULT 'http',  -- http, https, socks5
    
    -- Proxy details
    username VARCHAR(100),
    password TEXT,
    port INTEGER,
    
    -- Performance and reliability
    success_rate DECIMAL(5,4) DEFAULT 0.0,
    response_time_ms INTEGER,
    last_used TIMESTAMP,
    last_success TIMESTAMP,
    last_failure TIMESTAMP,
    consecutive_failures INTEGER DEFAULT 0,
    
    -- Usage limits
    max_requests_per_hour INTEGER DEFAULT 1000,
    current_requests_hour INTEGER DEFAULT 0,
    requests_reset_time TIMESTAMP,
    
    -- Status
    status VARCHAR(20) DEFAULT 'active',  -- active, banned, limited, testing, inactive
    is_premium BOOLEAN DEFAULT false,
    
    -- Cost tracking
    cost_per_request DECIMAL(10,6),
    monthly_cost_limit DECIMAL(10,2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tags TEXT[],
    
    UNIQUE(url, port),
    CONSTRAINT proxy_protocol_check CHECK (protocol IN ('http', 'https', 'socks5')),
    CONSTRAINT proxy_status_check CHECK (status IN ('active', 'ban-ned', 'limited', 'testing', 'inactive'))
);

-- Worker-proxy associations
CREATE TABLE worker_proxy_pools (
    id SERIAL PRIMARY KEY,
    worker_id INTEGER REFERENCES scraping_workers(id),
    proxy_id INTEGER REFERENCES proxy_servers(id),
    priority INTEGER DEFAULT 1,  -- Order of preference
    weight INTEGER DEFAULT 1,    -- Load balancing weight
    
    -- Usage configuration
    max_requests_per_run INTEGER DEFAULT 100,
    cooldown_seconds INTEGER DEFAULT 60,
    
    -- Status
    is_active BOOLEAN DEFAULT true,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(worker_id, proxy_id)
);

-- Indexes for performance
CREATE INDEX idx_scraping_workers_status ON scraping_workers(status);
CREATE INDEX idx_scraping_workers_next_run ON scraping_workers(next_run) WHERE status = 'active';
CREATE INDEX idx_scraping_workers_database ON scraping_workers(database_id);
CREATE INDEX idx_scraping_databases_active ON scraping_databases(is_active);
CREATE INDEX idx_worker_execution_history_worker_id ON worker_execution_history(worker_id);
CREATE INDEX idx_worker_execution_history_status ON worker_execution_history(status);
CREATE INDEX idx_worker_execution_history_timestamp ON worker_execution_history(execution_start);
CREATE INDEX idx_worker_schedule_log_worker_id ON worker_schedule_log(worker_id);
CREATE INDEX idx_worker_schedule_log_scheduled ON worker_schedule_log(scheduled_time) WHERE status = 'scheduled';
CREATE INDEX idx_worker_schedule_log_status ON worker_schedule_log(status);
CREATE INDEX idx_proxy_servers_status ON proxy_servers(status);
CREATE INDEX idx_proxy_servers_success_rate ON proxy_servers(success_rate DESC);

-- Views for easy management

-- Active workers view
CREATE VIEW active_workers_view AS
SELECT 
    w.id,
    w.name,
    w.description,
    w.site,
    w.search_term,
    w.location,
    w.country,
    w.results_per_run,
    w.schedule_hours,
    w.schedule_minute_offset,
    w.timezone,
    w.status,
    w.last_run,
    w.next_run,
    w.last_success,
    w.consecutive_errors,
    w.database_id,
    d.name as database_name,
    d.table_name,
    w.tags
FROM scraping_workers w
LEFT JOIN scraping_databases d ON w.database_id = d.id
WHERE w.status = 'active'
ORDER BY w.next_run;

-- Worker performance summary view
CREATE VIEW worker_performance_summary AS
SELECT 
    w.id,
    w.name,
    w.site,
    COUNT(eh.id) as total_executions,
    COUNT(CASE WHEN eh.status = 'success' THEN 1 END) as successful_executions,
    COUNT(CASE WHEN eh.status = 'failed' THEN 1 END) as failed_executions,
    COALESCE(AVG(eh.duration_seconds), 0) as avg_duration_seconds,
    COALESCE(SUM(eh.jobs_inserted), 0) as total_jobs_inserted,
    COALESCE(AVG(eh.jobs_inserted), 0) as avg_jobs_per_run,
    COALESCE(SUM(eh.jobs_skipped), 0) as total_jobs_skipped,
    COALESCE(SUM(eh.duplicates_found), 0) as total_duplicates,
    COALESCE(SUM(eh.network_errors), 0) as total_network_errors,
    COALESCE(SUM(eh.proxy_errors), 0) as total_proxy_errors,
    MAX(eh.execution_start) as last_execution,
    MAX(eh.last_success) as last_successful_run,
    CASE 
        WHEN COUNT(eh.id) > 0 
        THEN (COUNT(CASE WHEN eh.status = 'success' THEN 1 END) * 100.0 / COUNT(eh.id)) 
        ELSE 0 
    END as success_rate_percent,
    CASE 
        WHEN w.created_at IS NOT NULL 
        THEN DATEDIFF('minute', w.created_at, CURRENT_TIMESTAMP) / 1440.0  -- Days since creation
        ELSE 0 
    END as days_running
FROM scraping_workers w
LEFT JOIN worker_execution_history eh ON w.id = eh.worker_id
GROUP BY w.id, w.name, w.site, w.created_at
ORDER BY w.name;

-- Database health view
CREATE VIEW database_health_view AS
SELECT 
    d.id,
    d.name,
    d.database_name,
    d.host,
    d.port,
    d.is_active,
    d.connection_status,
    d.last_connection_test,
    d.connection_error,
    COUNT(DISTINCT w.id) as active_workers_count,
    COALESCE(MAX(eh.execution_start), d.last_connection_test) as last_activity,
    CASE 
        WHEN d.connection_status = 'success' AND d.is_active 
        THEN 'healthy'::text
        WHEN d.connection_status = 'failed' 
        THEN 'connection_failed'::text
        WHEN NOT d.is_active 
        THEN 'inactive'::text
        ELSE 'unknown'::text
    END as health_status
FROM scraping_databases d
LEFT JOIN scraping_workers w ON d.id = w.database_id AND w.status = 'active'
LEFT JOIN worker_execution_history eh ON d.id = eh.database_id
GROUP BY d.id, d.name, d.database_name, d.host, d.port, d.is_active, d.connection_status, d.last_connection_test, d.connection_error
ORDER BY d.name;

-- Proxy performance view
CREATE VIEW proxy_performance_view AS
SELECT 
    p.id,
    p.url,
    p country,
    p.provider,
    p.status,
    p.success_rate,
    p.response_time_ms,
    p.last_success,
    p.last_failure,
    p.consecutive_failures,
    p.max_requests_per_hour,
    p.current_requests_hour,
    p.requests_reset_time,
    p.is_premium,
    COUNT(wpp.id) as worker_count,
    CASE 
        WHEN p.status = 'active' AND p.success_rate > 0.9 
        THEN 'excellent'::text
        WHEN p.status = 'active' AND p.success_rate > 0.7 
        THEN 'good'::text
        WHEN p.status = 'active' AND p.success_rate > 0.5 
        THEN 'fair'::text
        WHEN p.status = 'active' 
        THEN 'poor'::text
        WHEN p.status = 'banned' 
        THEN 'banned'::text
        WHEN p.status = 'limited' 
        THEN 'rate_limited'::text
        ELSE 'inactive'::text
    END as performance_grade
FROM proxy_servers p
LEFT JOIN worker_proxy_pools wpp ON p.id = wpp.proxy_id AND wpp.is_active = true
GROUP BY p.id, p.url, p.country, p.provider, p.status, p.success_rate, p.response_time_ms, p.last_success, p.last_failure, p.consecutive_failures, p.max_requests_per_hour, p.current_requests_hour, p.requests_reset_time, p.is_premium
ORDER BY p.success_rate DESC, p.response_time_ms;

-- Triggers for automatic updates
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_scraping_workers_updated_at BEFORE UPDATE ON scraping_workers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_scraping_databases_updated_at BEFORE UPDATE ON scraping_databases
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_proxy_servers_updated_at BEFORE UPDATE ON proxy_servers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Function to calculate next run time
CREATE OR REPLACE FUNCTION calculate_next_run_time(
    last_run TIMESTAMP,
    schedule_hours INTEGER,
    schedule_minute_offset INTEGER,
    timezone VARCHAR
) RETURNS TIMESTAMP AS $$
BEGIN
    IF last_run IS NULL THEN
        RETURN (
            CURRENT_TIMESTAMP AT TIME ZONE timezone + 
            INTERVAL '1 minute' * schedule_minute_offset
        ) AT TIME ZONE 'UTC';
    ELSE
        RETURN (
            last_run AT TIME ZONE timezone + 
            INTERVAL '1 hour' * schedule_hours + 
            INTERVAL '1 minute' * schedule_minute_offset
        ) AT TIME ZONE 'UTC';
    END IF;
END;
$$ language 'plpgsql';

-- Function to auto-pause workers with too many consecutive errors
CREATE OR REPLACE FUNCTION auto_pause_failed_workers()
RETURNS VOID AS $$
BEGIN
    UPDATE scraping_workers 
    SET 
        status = 'paused',
        last_error = 'Auto-paused due to consecutive errors',
        updated_at = CURRENT_TIMESTAMP
    WHERE 
        status = 'active' AND
        consecutive_errors >= max_consecutive_errors AND
        auto_pause_on_errors = true;
END;
$$ language 'plpgsql';

-- Function to reset proxy hourly request counters
CREATE OR REPLACE FUNCTION reset_proxy_hourly_requests()
RETURNS VOID AS $$
BEGIN
    UPDATE proxy_servers 
    SET 
        current_requests_hour = 0,
        requests_reset_time = CURRENT_TIMESTAMP
    WHERE 
        requests_reset_time IS NULL OR
        requests_reset_time <= CURRENT_TIMESTAMP - INTERVAL '1 hour';
END;
$$ language 'plpgsql';

-- Sample data for testing
INSERT INTO scraping_databases (name, description, host, port, database_name, username, password, tags) VALUES
('Brazil Production', 'Primary database for Brazilian job listings', 'localhost', 5432, 'brazil_jobs', 'postgres', 'encrypted_password', ARRAY['brazil', 'production']),
('US Staging', 'Staging database for US market testing', 'localhost', 5433, 'us_jobs', 'postgres', 'encrypted_password', ARRAY['usa', 'staging']),
('European Production', 'Production database for European job listings', 'eu.example.com', 5432, 'europe_jobs', 'postgres', 'encrypted_password', ARRAY['europe', 'production']);

INSERT INTO worker_templates (name, description, site, category, difficulty, template_config) VALUES
('Brazil Tech Jobs', 'Scrape technology jobs in Brazil', 'linkedin', 'tech', 'medium', 
    '{"location": "Brazil", "job_type": ["FULL_TIME"], "results_per_run": 50, "schedule_hours": 24, "search_term": "developer OR programador OR engenheiro"}'),
('Remote Senior Positions', 'Senior-level remote jobs', 'indeed', 'remote', 'medium',
    '{"location": "Remote", "job_type": ["FULL_TIME"], "results_per_run": 30, "schedule_hours": 12, "search_term": "senior OR lead OR principal"}'),
('Entry Level Brazil', 'Entry-level positions in Brazilian market', 'glassdoor', 'entry-level', 'easy',
    '{"location": "Brazil", "job_type": ["FULL_TIME", "INTERNSHIP"], "results_per_run": 40, "schedule_hours": 48, "search_term": "junior OR estagio OR trainee"}');

COMMIT;

-- Schema version 2 - Force rebuild 2025-08-29