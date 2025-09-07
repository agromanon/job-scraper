# JobSpy - Job Scraping Administration System

## Project Overview

JobSpy is a comprehensive, modular job scraping and monitoring system built with Flask, PostgreSQL, and Docker. The system provides a web-based administrative interface for managing automated job scraping workers across multiple job sites and databases.

## Current Development Status

### ✅ **Completed Features**

#### Core Infrastructure
- **Flask Admin Interface**: Complete web-based admin panel with authentication
- **Database Architecture**: Multi-database support with PostgreSQL and connection pooling
- **Docker Deployment**: Production-ready containerized deployment with docker-compose
- **Light Theme UI**: Professional, accessible user interface
- **Error Handling**: Comprehensive error logging and user feedback

#### Database Schema (`modular_schema.sql`)
- **`scraping_workers`**: Worker configurations with scheduling and resource limits
- **`scraping_databases`**: Multi-database connection management
- **`worker_execution_history`**: Execution metrics and performance tracking
- **`worker_schedule_log`**: Scheduling and execution monitoring
- **`worker_templates`**: Pre-configured worker templates for quick setup
- **`proxy_servers`**: Proxy management with rotation policies
- **`worker_proxy_pools`**: Proxy-worker associations
- **Views & Functions**: Performance monitoring, health checks, and automated maintenance

#### Worker Management
- **CRUD Operations**: Create, Read, Update, Delete workers
- **Multi-Site Support**:
  - LinkedIn
  - Indeed  
  - Glassdoor
  - Google Jobs
  - Zip Recruiter
  - Bayt
  - Naukri
  - BDJobs
- **Scheduling**: Timezone-aware scheduling with staggered execution
- **Resource Controls**: Memory, CPU, and runtime limits
- **Search Configuration**: Advanced search terms, locations, job types
- **Error Handling**: Auto-pause on consecutive errors with retry limits

#### Database Management
- **Multi-Database Support**: Manage multiple PostgreSQL databases
- **Connection Testing**: Real-time database connection health monitoring
- **Performance Tuning**: Batch size, connection pools, deduplication strategies
- **SSL Configuration**: Flexible SSL mode options for secure connections
- **CRUD Operations**: Full database lifecycle management

#### Authentication & Security
- **Password-Protected Admin Panel**: Secure login access
- **Session Management**: Flask-Login based authentication
- **Form Validation**: CSRF protection and input validation
- **Database Security**: Encrypted password storage, SSL connections

### ⚠️ **Partially Implemented Features**

#### Integration Layer
- **Worker Engine**: Basic framework present (`worker_manager.py`)
- **Scraping Engine**: JobSpy core library integrated
- **Performance Monitoring**: Metrics collection in place, reporting incomplete

#### Monitoring & Observability
- **Execution History**: Database schema ready, UI integration pending
- **Performance Metrics**: Data collection implemented, visualization incomplete
- **Health Monitoring**: Basic health checks implemented, detailed monitoring pending

### 🔄 **Pending Features**

#### Execution Engine
- **Worker Scheduler**: Automated scheduled execution (framework ready, needs implementation)
- **Scraper Integration**: Full integration with JobSpy scraping library
- **Proxy Management**: Proxy rotation and failover logic
- **Rate Limiting**: Respect site-specific rate limiting policies

#### Advanced Features
- **Dashboard Analytics**: Real-time performance dashboards and charts
- **Alert System**: Email/SMS notifications for failures
- **API Integration**: RESTful API for external integrations
- **Job Data Processing**: Data enrichment, deduplication, and export

#### DevOps & Scaling
- **Kubernetes Deployment**: Helm charts and K8s manifests
- **Infrastructure as Code**: Terraform configurations for cloud deployment
- **Monitoring Stack**: Prometheus, Grafana, and alerting setup
- **Load Balancing**: Multi-instance deployment and scaling

## Technical Architecture

### Backend Stack

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Flask Admin   │◄──►│   PostgreSQL    │◄──►│   Worker Engine │
│   Interface     │    │   Database      │    │   Processing   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Redis Cache   │    │   JobSpy Core  │    │   Scraping      │
│   Session       │    │   Library       │    │   Workers       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Key Components

#### 1. **Flask Admin Interface** (`worker_admin.py`)
- **Routes**: Dashboard, Workers, Databases management
- **Forms**: Flask-WTF forms with validation
- **Authentication**: @login_required decorator protection
- **Templates**: Jinja2 with Bootstrap 5 integration
- **API Health**: `/health` endpoint for monitoring

#### 2. **Database Layer** (`modular_schema.sql`)
- **Tables**: Normalized schema with foreign key relationships
- **Indexing**: Performance-optimized indexes
- **Constraints**: Data integrity and validation
- **Views**: Aggregated data for performance monitoring
- **Functions**: Automated maintenance and calculations

#### 3. **Worker Engine** (`worker_manager.py`)
- **WorkerConfig**: Worker configuration and settings
- **WorkerManager**: Orchestration and execution
- **Performance Tracking**: Resource monitoring and metrics
- **Error Handling**: Comprehensive exception management

#### 4. **Docker Deployment** (`docker-compose.yml`)
- **Frontend**: Flask admin application
- **Worker**: Scraper execution engine
- **Scheduler**: Automated worker scheduling
- **Monitoring**: Grafana and Prometheus integration
- **Load Balancer**: Nginx reverse proxy

### Database Schema Overview

```sql
-- Core Worker Configuration
scraping_workers (core worker settings, scheduling, resource limits)
    ├── scraping_databases (database connection details)
    ├── worker_execution_history (execution metrics and performance)
    ├── worker_schedule_log (scheduling compliance)
    │
    └── worker_proxy_pools (proxy assignments)
        └── proxy_servers (proxy rotation and performance)

-- Support Tables
worker_templates (pre-configured worker setups)
```

### Configuration Files

#### `worker_admin.py`
- **Database Connection**: PostgreSQL connection pooling
- **Form Classes**: WorkerForm, DatabaseForm, ProxyForm
- **Route Handlers**: CRUD operations and API endpoints
- **Authentication**: Password-based access control

#### `Dockerfile.force_rebuild_ultimate`
- **Base Image**: Python 3.11-slim
- **Dependencies**: JobSpy, Flask, PostgreSQL drivers
- **Templates**: Auto-generated Bootstrap 5 templates
- **Environment**: Production-ready configuration

#### `docker-compose.yml`
- **Services**: Frontend, Worker, Scheduler, Monitoring
- **Networking**: Custom bridge network with subnet
- **Volumes**: Persistent data storage
- **Health Checks**: Container health monitoring

## Development Environment

### Quick Start
1. **Environment Setup**: EasyPanel VPS deployment
2. **Database**: PostgreSQL with modular schema
3. **Dependencies**: pip install -r requirements.admin.txt
4. **Configuration**: Environment variables for database/auth
5. **Deployment**: Docker Compose orchestration

### EasyPanel Deployment
The JobSpy system is designed for deployment on EasyPanel, which provides a simplified approach to managing the application compared to complex deployment scripts.

#### Deployment Process
1. Code changes are committed to GitHub
2. EasyPanel automatically detects new commits and rebuilds the application
3. Services are managed through EasyPanel's web interface
4. Environment variables are configured through the EasyPanel dashboard
5. Monitoring and logs are available through the web interface

#### Key Benefits
- **Simplified Management**: No complex bash scripts needed
- **Automatic Deployments**: Push to deploy from Git
- **Built-in Monitoring**: Health checks and resource usage monitoring
- **Zero-downtime Updates**: Seamless application updates
- **Easy Scaling**: Duplicate services for additional workers

### Key Environment Variables
```bash
POSTGRES_URL=postgres://user:pass@host:port/dbname?sslmode=require
REDIS_URL=redis://localhost:6379/0
SECRET_KEY=your-secret-key-here
ADMIN_PASSWORD=admin123
```

### Database Tables Status

| Table | Status | Purpose |
|-------|--------|---------|
| `scraping_workers` | ✅ Complete | Worker configurations |
| `scraping_databases` | ✅ Complete | Database connections |
| `worker_execution_history` | ✅ Complete | Execution metrics |
| `worker_schedule_log` | ✅ Complete | Scheduling compliance |
| `worker_templates` | ✅ Complete | Worker templates |
| `proxy_servers` | ✅ Complete | Proxy management |
| `worker_proxy_pools` | ✅ Complete | Worker-proxy mapping |

## API Routes & Endpoints

### Web Interface Routes
- `GET /` - Dashboard overview
- `GET /login` - Authentication
- `GET /workers` - Workers list
- `GET /workers/new` - Create worker
- `GET /workers/<id>/edit` - Edit worker
- `POST /workers/<id>/delete` - Delete worker
- `GET /databases` - Databases list
- `GET /databases/new` - Create database
- `POST /databases/<id>/test` - Test database connection
- `GET /databases/<id>/edit` - Edit database
- `POST /databases/<id>/delete` - Delete database

### API Endpoints
- `GET /health` - Health check
- `GET /api/workers/<id>/history` - Worker execution history
- `GET /api/workers/<id>/performance` - Worker performance metrics
- `POST /api/databases/<id>/test` - Database connection test

## Known Issues & Future Work

### Current Limitations
1. **Worker Execution**: Admin interface complete, but actual scraping execution needs integration
2. **Scheduling**: UI ready, background scheduler needs implementation
3. **Real-time Updates**: No WebSocket implementation for live updates
4. **Bulk Operations**: No support for bulk worker/database operations
5. **Import/Export**: No configuration import/export functionality

### Technical Debt
1. **Error Handling**: Some error cases need more graceful handling
2. **Configuration**: Some magic numbers need to be configurable
3. **Database Optimization**: Query optimization for large datasets
4. **Security**: Additional security hardening recommended

### Next Development Priorities
1. **Worker Execution Engine**: Complete scraping worker implementation
2. **Background Scheduler**: Integrate with Flask for automated execution
3. **Performance Dashboard**: Real-time metrics and visualizations
4. **Alert System**: Failure notifications and automated recovery
5. **API Development**: External API for third-party integrations

## Deployment Architecture

### Production Deployment (Docker Compose)
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend      │    │   PostgreSQL    │    │   Redis         │
│   (Port 5000)   │    │   (Port 5432)   │    │   (Port 6379)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   Nginx         │
                    │   (Port 80/443) │
                    └─────────────────┘
```

### EasyPanel Deployment
The JobSpy system is deployed using EasyPanel, which provides a simplified approach to managing the application:

1. **Code Updates**: Commits to GitHub are automatically detected by EasyPanel
2. **Service Management**: Services are managed through EasyPanel's web interface
3. **Environment Configuration**: Environment variables are configured through the dashboard
4. **Monitoring**: Built-in health checks and resource usage monitoring
5. **Scaling**: Services can be easily duplicated for additional workers

### Networking
- **Frontend Network**: `172.20.0.0/16` (custom bridge)
- **Database Ports**: PostgreSQL (5432)
- **Redis Port**: 6379
- **Admin Port**: 5000 (HTTP)

### Data Persistence
- **PostgreSQL Data**: `/var/lib/postgresql/data`
- **Redis Data**: `redis_data` volume
- **Configuration**: Environment variables and config files
- **Logs**: Bound to host directory for persistence

## Version History

### Recent Updates
- **v2.0.0**: Complete UI overhaul with light theme
  - Fixed worker creation database issues
  - Added database deletion functionality
  - Switched from dark to light professional theme
  - Improved form validation and error handling

- **v1.8.5**: Database schema improvements
  - Added execution history tracking
  - Enhanced performance monitoring
  - Multi-database optimization

- **v1.7.0**: Docker deployment improvements
  - Force rebuild Dockerfile for reliable deployments
  - Health checks and monitoring
  - EasyPanel deployment optimization

### Future Roadmap
- **Q4 2025**: Complete worker execution engine
- **Q1 2026**: Advanced analytics and reporting
- **Q2 2026**: API development and integrations
- **Q3 2026**: Kubernetes deployment options

## Troubleshooting Common Issues

### Worker Creation Fails
- **Cause**: Missing `status` field in database operations
- **Solution**: Ensure `modular_schema.sql` includes all fields
- **Check**: WorkerForm field definitions match database schema

### Database Connection Issues
- **Cause**: SSL configuration or connection parameters
- **Solution**: Test with different SSL modes (`disable`, `require`, `verify-full`)
- **Check**: Database credentials and network connectivity

### Template Rendering Errors
- **Cause**: Missing templates in Docker build
- **Solution**: Use `Dockerfile.force_rebuild_ultimate` for complete template generation
- **Check**: All templates are included in the Dockerfile template creation section

### Docker Cache Issues
- **Cause**: Layer caching preventing updates
- **Solution**: Use `--no-cache` flag or the force rebuild Dockerfile
- **Check**: Custom build arguments are properly generated

## Development Session Continuation

When starting a new development session with limited context, focus on these priority areas:

### Immediate Issues to Check
1. **Database Connection**: Verify POSTGRES_URL and schema initialization
2. **Template Generation**: Ensure all templates are created during Docker build
3. **Form Field Alignment**: Check WorkerForm fields match database schema
4. **Route Functionality**: Verify all CRUD routes are working

### Development Workflow
```bash
# 1. Environment Setup
git pull origin main
docker-compose build --no-cache frontend
docker-compose up -d

# 2. Database Migration
Check logs for schema initialization errors
Apply fix_missing_table.sql if needed

# 3. Form Validation
Test worker creation with all form fields
Verify database operations complete successfully

# 4. UI Testing
Test all CRUD operations for workers and databases
Verify responsive design on mobile devices
```

### EasyPanel Deployment Process
1. **Code Updates**: Commit changes to GitHub
2. **Automatic Deployment**: EasyPanel detects new commits and rebuilds the application
3. **Service Management**: Manage services through EasyPanel's web interface
4. **Environment Configuration**: Update environment variables through the dashboard
5. **Monitoring**: Check service health and logs through the web interface

### Critical Files to Review
- `worker_admin.py`: Main application logic and routes
- `Dockerfile.force_rebuild_ultimate`: Template generation and build process
- `modular_schema.sql`: Database schema and constraints
- `docker-compose.yml`: Service orchestration and networking
- `EASYPANEL_DEPLOYMENT.md`: EasyPanel-specific deployment instructions

### Common Development Tasks
1. **Adding New Fields**: Update Form class + database schema + SQL statements
2. **Template Issues**: Regenerate in Dockerfile.force_rebuild_ultimate and rebuild
3. **Database Schema**: Use fix_missing_table.sql for quick fixes, update modular_schema.sql for permanent changes
4. **Docker Updates**: Always use force rebuild Dockerfile to ensure templates are updated
5. **EasyPanel Configuration**: Update deployment documentation when making deployment-related changes

This architecture provides a solid foundation for a production-ready job scraping system with current focus on completing the execution engine and monitoring components.