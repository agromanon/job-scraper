# Job Scraper - Modular Scraping System

A flexible, modular job scraping system that allows configuring specialized workers for different job sites with individual scheduling, database routing, and proxy management.

## Architecture Overview

The system consists of several components:

### Core Components

1. **Web Admin Interface** (Flask-based)
   - Worker configuration and management
   - Database connection management
   - Proxy management and monitoring
   - Real-time execution monitoring

2. **Worker Manager/Scheduler**
   - Centralized worker orchestration
   - Scheduled execution with configurable intervals
   - Multi-database support with routing
   - Error handling and retry logic

3. **Worker Engines**
   - Individual job scraping workers
   - Configurable per worker (site, filters, limits)
   - Independent execution with resource limits
   - Full JobSpy library integration

4. **Monitoring Stack**
   - Prometheus for metrics collection
   - Grafana for visualization
   - Nginx reverse proxy with SSL
   - Redis for caching and session management

### Database Schema

The system uses a comprehensive database schema with the following main tables:

- `scraping_workers` - Individual worker configurations
- `scraping_databases` - Database connection settings
- `worker_execution_history` - Execution tracking and monitoring
- `proxy_servers` - Proxy management and performance tracking
- `worker_proxy_pools` - Worker-proxy associations

## Features

### Modular Worker Configuration

- **Site-Specific Workers**: Configure workers for LinkedIn, Indeed, Glassdoor, Google Jobs, ZipRecruiter, Bayt, Naukri, and BDJobs
- **Flexible Scheduling**: Set individual schedules with minute offsets for staggered execution
- **Custom Filters**: Configure search terms, locations, job types, remote work preferences
- **Resource Limits**: Control memory, CPU, and execution time per worker
- **Error Handling**: Auto-pause on consecutive errors, configurable retry logic

### Multi-Database Support

- **Database Routing**: Route workers to different databases based on configuration
- **Connection Management**: Connection pooling and health monitoring
- **Deduplication**: Configurable deduplication strategies (unique ID, composite key)
- **Performance Optimization**: Batch processing and transaction management

### Proxy Management

- **Multiple Proxies**: Support for HTTP, HTTPS, and SOCKS5 proxies
- **Rotation Policies**: Rotating, sticky, or static proxy assignment per worker
- **Performance Tracking**: Success rate monitoring and automatic failover
- **Cost Management**: Request limits and cost tracking for premium proxies

### Monitoring and Analytics

- **Real-time Monitoring**: Live execution status and performance metrics
- **Historical Data**: Execution history with success rates and performance trends
- **Alert System**: Automatic alerts for failures and performance issues
- **Grafana Dashboards**: Pre-built dashboards for system monitoring

## Quick Start

### Prerequisites

- Docker and Docker Compose
- PostgreSQL database (external or managed)
- Hostinger VPS with EasyPanel (for production deployment)

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd jobspy-modular
   ```

2. **Configure environment**:
   ```bash
   # Copy .env template
   cp .env.example .env
   
   # Edit .env with your database credentials
   # For Hostinger VPS:
   POSTGRES_URL=postgres://aromanon:Afmg2486!@my-job-scraper_my-job-scraper:5432/job-data?sslmode=disable
   REDIS_URL=redis://default:Afmg2486!@my-job-scraper_my-job-scraper_redis:6379
   ```

3. **Run deployment script**:
   ```bash
   ./deploy.sh
   ```

### Manual Deployment

1. **Create directories**:
   ```bash
   mkdir -p logs monitoring ssl templates static/css static/js
   ```

2. **Generate SSL certificates**:
   ```bash
   openssl req -x509 -newkey rsa:4096 -keyout ssl/key.pem -out ssl/cert.pem -days 365 -nodes
   ```

3. **Start services**:
   ```bash
   docker-compose up -d
   ```

4. **Initialize database schema**:
   ```bash
   docker-compose exec web_admin python worker_manager.py
   ```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `POSTGRES_URL` | Database connection URL | Required |
| `REDIS_URL` | Redis connection URL | Required |
| `SECRET_KEY` | Flask secret key | Auto-generated |
| `GRAFANA_PASSWORD` | Grafana admin password | `admin` |
| `MAX_WORKERS` | Maximum concurrent workers | `10` |
| `LOG_LEVEL` | Logging level | `INFO` |

### Worker Configuration Example

Create a LinkedIn worker for Brazilian tech jobs:

```yaml
- name: "Brazil Tech Jobs"
  site: "linkedin"
  search_term: "developer OR programador OR engenheiro"
  location: "Brazil"
  job_type: ["FULL_TIME"]
  is_remote: false
  results_per_run: 50
  schedule_hours: 24
  schedule_minute_offset: 0  # Runs at 1:00 AM
  database_id: 1
  table_name: "brazil_job_listings"
  proxy_rotation_policy: "rotating"
  max_retries: 3
  timeout: 30
  status: "active"
```

### Database Configuration Example

Add a database connection for Brazilian market:

```yaml
- name: "Brazil Production"
  host: "localhost"
  database_name: "brazil_jobs"
  username: "postgres"
  ssl_mode: "require"
  deduplication_method: "unique_id"
  deduplication_fields: ["job_url"]
  batch_size: 100
```

## Usage

### Web Interface

1. **Access the admin interface**: Navigate to `https://localhost`
2. **Configure databases**: Add your target databases in the Databases section
3. **Create worker templates**: Define reusable worker configurations
4. **Deploy workers**: Create workers with specific schedules and filters
5. **Monitor execution**: View real-time execution status and performance metrics

### Worker Templates

Create templates for common use cases:

- **Brazil Tech Jobs**: Technology positions in Brazilian market
- **Remote Senior Positions**: Senior-level remote jobs globally
- **Entry Level Brazil**: Entry-level positions for Brazilian market
- **Finance Jobs**: Finance and accounting positions

### Scheduling

Workers can be scheduled with flexible timing:

- **Schedule Hours**: How often to run (1-168 hours)
- **Minute Offset**: Stardagger execution to avoid conflicts (0-59 minutes)
- **Timezone**: Worker-specific timezone configuration
- **Auto-Pause**: Automatically pause on consecutive errors

### Database Routing

Configure workers to use different databases:

```sql
-- Worker for Brazilian market
INSERT INTO scraping_workers (name, database_id, table_name, ...)
VALUES ('Brazil Tech Jobs', 1, 'brazil_job_listings', ...);

-- Worker for US market
INSERT INTO scraping_workers (name, database_id, table_name, ...)
VALUES ('US Tech Jobs', 2, 'us_job_listings', ...);
```

## Monitoring

### Grafana Dashboards

Access Grafana at `http://localhost:3000` with credentials `admin/admin`:

- **Worker Performance**: Success rates, execution times, job counts
- **Database Health**: Connection status, query performance, replication lag
- **Proxy Performance**: Success rates, response times, error rates
- **System Resources**: CPU, memory, disk usage metrics

### Prometheus Metrics

Access Prometheus at `http://localhost:9090` for:

- **Worker Metrics**: Execution counts, success/failure rates
- **Database Metrics**: Connection pool status, query performance
- **System Metrics**: Resource usage, container health
- **Custom Metrics**: Application-specific metrics

### Logging

Logs are available for all services:

```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f web_admin
docker-compose logs -f worker_scheduler

# View worker execution logs
docker-compose logs -f worker_engine_1
```

## Scaling

### Horizontal Scaling

Add more worker engines:

```yaml
# docker-compose.yml
worker_engine_3:
  build:
    context: .
    dockerfile: Dockerfile.force_rebuild_ultimate
  # ... same configuration
```

### Database Scaling

- **Read Replicas**: Configure read-only replicas for reporting
- **Connection Pooling**: Optimize pool sizes for high concurrency
- **Sharding**: Distribute workers across multiple databases

### Performance Tuning

- **Memory Limits**: Adjust per-worker memory allocation
- **CPU Limits**: Control CPU usage per worker
- **Network Optimization**: Configure proxy rotation and connection reuse
- **Batch Processing**: Optimize batch sizes for database operations

## Troubleshooting

### Common Issues

**Database Connection Issues**:
```bash
# Test database connection
docker-compose exec web_admin python -c "
import psycopg2
conn = psycopg2.connect('your_connection_string')
conn.close()
print('Database connection successful')
"
```

**Worker Execution Failures**:
```bash
# Check worker logs
docker-compose logs -f worker_scheduler
docker-compose logs -f worker_engine_1
```

**Service Health Issues**:
```bash
# Check service status
docker-compose ps
docker-compose logs -f [service_name]
```

### Performance Issues

- **High Memory Usage**: Reduce concurrent workers or increase worker memory limits
- **Slow Execution**: Optimize database queries or increase batch sizes
- **Rate Limiting**: Adjust proxy rotation policies or add more proxies
- **Database Performance**: Tune PostgreSQL configuration or add read replicas

### Network Issues

- **Proxy Failures**: Test proxy connections and update rotation policies
- **SSL Issues**: Verify SSL certificates and check network configuration
- **Firewall Rules**: Ensure proper port access between containers

## Security

### Best Practices

- **Use HTTPS**: Always use SSL/TLS for admin interface
- **Environment Variables**: Store sensitive data in environment variables
- **Network Security**: Use internal networks for inter-service communication
- **Database Security**: Use connection pooling and encryption in transit

### Access Control

- **Authentication**: Implement user authentication for admin interface
- **Authorization**: Control access to worker operations and database connections
- **Audit Logging**: Track all administrative actions and system changes

## API Reference

### Worker Management API

```bash
# Get worker history
GET /api/workers/{worker_id}/history

# Get worker performance stats
GET /api/workers/{worker_id}/performance

# Test database connection
POST /api/databases/{database_id}/test

# Toggle worker status
POST /api/workers/{worker_id}/toggle

# Execute worker manually
POST /api/workers/{worker_id}/execute
```

### Database API

```bash
# List databases
GET /api/databases

# Test database connection
POST /api/databases/{database_id}/test

# Get database health
GET /api/databases/{database_id}/health
```

## Contributing

### Development Setup

1. **Clone repository** and install development dependencies
2. **Setup local environment** with Docker Compose
3. **Run tests**: `python -m pytest tests/`
4. **Lint code**: `flake8 .`
5. **Format code**: `black .`

### Code Style

- Follow PEP 8 guidelines
- Use type hints where applicable
- Write comprehensive docstrings
- Include unit tests for new features

## Deployment on Hostinger VPS

### Prerequisites

- Hostinger VPS with EasyPanel
- Docker and Docker Compose installed
- PostgreSQL database configured
- Redis instance configured

### Deployment Steps

1. **Upload files** to your VPS
2. **Configure .env** with your database and Redis credentials
3. **Run deployment script**: `./deploy.sh`
4. **Configure EasyPanel** points to your services

### EasyPanel Configuration

Add the following services to EasyPanel:

- **Web Admin**: Proxy to port 5000 (web_admin service)
- **Grafana**: Proxy to port 3000 (grafana service)
- **Prometheus**: Proxy to port 9090 (prometheus service)

### Service Configuration

Ensure these ports are available in your VPS:

- 80/443: Nginx reverse proxy
- 5000: Flask web admin
- 3000: Grafana dashboard
- 9090: Prometheus metrics

## License

This project is licensed under the MIT License. See LICENSE file for details.

## Support

For support and questions:

- Create an issue in the repository
- Check the troubleshooting section
- Review the logging information
- Monitor Grafana dashboards for system health

## Changelog

### Version 1.0.0
- Initial modular scraping system
- Multi-database support with routing
- Web admin interface for worker configuration
- Comprehensive monitoring stack
- Proxy management and rotation system
- Docker-based deployment with EasyPanel compatibility
- Hostinger VPS support with external database and Redis