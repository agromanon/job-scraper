#!/bin/bash

# Job Scraper VPS Deployment Script
# Optimized for Hostinger VPS with EasyPanel

set -e

echo "🚀 Job Scraper VPS Deployment"
echo "============================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[WARN] $1${NC}"
}

error() {
    echo -e "${RED}[ERROR] $1${NC}"
}

# Check if we're on the VPS
check_vps() {
    log "Checking VPS environment..."
    
    # Check if PostgreSQL is accessible
    if ! python3 -c "
import psycopg2
import os
try:
    conn = psycopg2.connect(os.getenv('POSTGRES_URL'))
    conn.close()
    print('✓ Database connection successful')
except Exception as e:
    print(f'✗ Database connection failed: {e}')
    exit(1)
" 2>/dev/null; then
        error "PostgreSQL connection failed"
        exit 1
    fi
    
    # Check if Redis is accessible
    if ! python3 -c "
import redis
import os
try:
    r = redis.from_url(os.getenv('REDIS_URL'))
    r.ping()
    print('✓ Redis connection successful')
except Exception as e:
    print(f'✗ Redis connection failed: {e}')
    exit(1)
" 2>/dev/null; then
        error "Redis connection failed"
        exit 1
    fi
    
    log "✓ VPS environment check passed"
}

# Create necessary directories
setup_directories() {
    log "Creating necessary directories..."
    
    mkdir -p logs monitoring ssl templates static/css static/js monitoring/grafana/provisioning/datasources monitoring/grafana/provisioning/dashboards
    chmod 755 logs templates static
    
    log "✓ Directories created"
}

# Generate SSL certificates (self-signed for development)
setup_ssl() {
    log "Setting up SSL certificates..."
    
    if [ ! -f ssl/cert.pem ] || [ ! -f ssl/key.pem ]; then
        openssl req -x509 -newkey rsa:4096 -keyout ssl/key.pem -out ssl/cert.pem -days 365 -nodes \
            -subj "/C=BR/ST=SP/L=Sao Paulo/O=JobScraper/OU=Dev/CN=localhost" \
            -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" 2>/dev/null || true
        chmod 600 ssl/key.pem ssl/cert.pem
        log "✓ SSL certificates generated"
    else
        log "✓ SSL certificates already exist"
    fi
}

# Create basic HTML templates
create_templates() {
    log "Creating HTML templates..."
    
    # Create base template
    cat > templates/base.html << 'EOF'
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{% block title %}Job Scraper Admin{% endblock %}</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.8.1/font/bootstrap-icons.css" rel="stylesheet">
    <link href="/static/css/style.css" rel="stylesheet">
</head>
<body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-primary">
        <div class="container">
            <a class="navbar-brand" href="{{ url_for('dashboard') }}">
                <i class="bi bi-briefcase-fill"></i> Job Scraper Admin
            </a>
            <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav">
                <span class="navbar-toggler-icon"></span>
            </button>
            <div class="collapse navbar-collapse" id="navbarNav">
                <ul class="navbar-nav">
                    <li class="nav-item">
                        <a class="nav-link" href="{{ url_for('dashboard') }}">Dashboard</a>
                    </li>
                    <li class="nav-item">
                        <a class="nav-link" href="{{ url_for('list_workers') }}">Workers</a>
                    </li>
                    <li class="nav-item">
                        <a class="nav-link" href="{{ url_for('list_databases') }}">Databases</a>
                    </li>
                </ul>
                <ul class="navbar-nav ms-auto">
                    <li class="nav-item">
                        <a class="nav-link" href="/health">Health</a>
                    </li>
                </ul>
            </div>
        </div>
    </nav>

    <div class="container mt-4">
        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
                {% for category, message in messages %}
                    <div class="alert alert-{{ 'danger' if category == 'error' else 'success' }} alert-dismissible fade show" role="alert">
                        {{ message }}
                        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
                    </div>
                {% endfor %}
            {% endif %}
        {% endwith %}

        {% block content %}{% endblock %}
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
    <script src="/static/js/app.js"></script>
    {% block scripts %}{% endblock %}
</body>
</html>
EOF

    # Create dashboard template
    cat > templates/dashboard.html << 'EOF'
{% extends "base.html" %}

{% block title %}Dashboard - Job Scraper Admin{% endblock %}

{% block content %}
<h1 class="mb-4">Dashboard</h1>

<div class="row mb-4">
    <div class="col-md-3">
        <div class="card text-center">
            <div class="card-body">
                <h2 class="text-primary">{{ stats.get('total_workers', 0) }}</h2>
                <p class="text-muted">Total Workers</p>
            </div>
        </div>
    </div>
    <div class="col-md-3">
        <div class="card text-center">
            <div class="card-body">
                <h2 class="text-success">{{ stats.get('active_workers', 0) }}</h2>
                <p class="text-muted">Active Workers</p>
            </div>
        </div>
    </div>
    <div class="col-md-3">
        <div class="card text-center">
            <div class="card-body">
                <h2 class="text-warning">{{ stats.get('paused_workers', 0) }}</h2>
                <p class="text-muted">Paused Workers</p>
            </div>
        </div>
    </div>
    <div class="col-md-3">
        <div class="card text-center">
            <div class="card-body">
                <h2 class="text-info">{{ stats.get('workers_today', 0) }}</h2>
                <p class="text-muted">Workers Today</p>
            </div>
        </div>
    </div>
</div>

<div class="row">
    <div class="col-md-6">
        <div class="card">
            <div class="card-header d-flex justify-content-between align-items-center">
                <h5 class="mb-0">Recent Executions</h5>
                <button class="btn btn-sm btn-outline-primary" onclick="location.reload();">
                    <i class="bi bi-arrow-clockwise"></i> Refresh
                </button>
            </div>
            <div class="card-body">
                {% if recent_executions %}
                    <div class="table-responsive">
                        <table class="table table-sm table-hover">
                            <thead>
                                <tr>
                                    <th>Worker</th>
                                    <th>Status</th>
                                    <th>Time</th>
                                    <th>Jobs Found</th>
                                </tr>
                            </thead>
                            <tbody>
                                {% for exec in recent_executions %}
                                    <tr>
                                        <td>{{ exec.get('worker_name', 'N/A') }}</td>
                                        <td>
                                            <span class="badge bg-{% if exec.get('status') == 'success' %}success{% elif exec.get('status') == 'failed' %}danger{% else %}warning{% endif %}">
                                                {{ exec.get('status', 'N/A') }}
                                            </span>
                                        </td>
                                        <td>{{ exec.get('execution_start', '').strftime('%H:%M %d/%m') if exec.get('execution_start') else 'N/A' }}</td>
                                        <td>{{ exec.get('jobs_found', 0) }}</td>
                                    </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                {% else %}
                    <div class="text-center text-muted py-4">
                        <i class="bi bi-clock-history" style="font-size: 3rem;"></i>
                        <p class="mt-2">No recent executions</p>
                        <p class="small">Workers will appear here after execution</p>
                    </div>
                {% endif %}
            </div>
        </div>
    </div>
    
    <div class="col-md-6">
        <div class="card">
            <div class="card-header">
                <h5 class="mb-0">Scheduled Runs</h5>
            </div>
            <div class="card-body">
                {% if scheduled_runs %}
                    <div class="table-responsive">
                        <table class="table table-sm table-hover">
                            <thead>
                                <tr>
                                    <th>Worker</th>
                                    <th>Site</th>
                                    <th>Next Run</th>
                                    <th>Database</th>
                                </tr>
                            </thead>
                            <tbody>
                                {% for run in scheduled_runs %}
                                    <tr>
                                        <td>{{ run.get('name', 'N/A') }}</td>
                                        <td>{{ run.get('site', 'N/A') }}</td>
                                        <td>{{ run.get('next_run', '').strftime('%H:%M %d/%m') if run.get('next_run') else 'N/A' }}</td>
                                        <td>{{ run.get('database_name', 'N/A') }}</td>
                                    </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                {% else %}
                    <div class="text-center text-muted py-4">
                        <i class="bi bi-calendar3" style="font-size: 3rem;"></i>
                        <p class="mt-2">No scheduled workers</p>
                        <p class="small">Create your first worker to get started</p>
                        <a href="{{ url_for('new_worker') }}" class="btn btn-primary btn-sm mt-2">
                            <i class="bi bi-plus-circle"></i> Create Worker
                        </a>
                    </div>
                {% endif %}
            </div>
        </div>
    </div>
</div>

<div class="row mt-4">
    <div class="col-12">
        <div class="card">
            <div class="card-header">
                <h5 class="mb-0">Quick Actions</h5>
            </div>
            <div class="card-body">
                <div class="d-flex gap-3 flex-wrap">
                    <a href="{{ url_for('new_worker') }}" class="btn btn-primary">
                        <i class="bi bi-plus-lg"></i> Create Worker
                    </a>
                    <a href="{{ url_for('new_database') }}" class="btn btn-outline-primary">
                        <i class="bi bi-database-add"></i> Add Database
                    </a>
                    <button class="btn btn-outline-secondary" onclick="testConnections()">
                        <i class="bi bi-activity"></i> Test Connections
                    </button>
                    <button class="btn btn-outline-info" onclick="location.reload()">
                        <i class="bi bi-arrow-clockwise"></i> Refresh Dashboard
                    </button>
                </div>
            </div>
        </div>
    </div>
</div>
{% endblock %}

{% block scripts %}
<script>
function testConnections() {
    // Test database connection
    fetch('/api/databases/test-all')
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                alert('All connections tested successfully');
            } else {
                alert('Connection test failed: ' + data.error);
            }
        })
        .catch(error => {
            console.error('Error:', error);
            alert('Error testing connections');
        });
}
</script>
{% endblock %}
EOF

    # Create workers list template
    cat > templates/workers.html << 'EOF'
{% extends "base.html" %}

{% block title %}Workers - Job Scraper Admin{% endblock %}

{% block content %}
<div class="d-flex justify-content-between align-items-center mb-4">
    <h1 class="mb-0">Workers</h1>
    <a href="{{ url_for('new_worker') }}" class="btn btn-primary">
        <i class="bi bi-plus-lg"></i> Create Worker
    </a>
</div>

<div class="card">
    <div class="card-body">
        {% if workers %}
            <div class="table-responsive">
                <table class="table table-striped table-hover">
                    <thead>
                        <tr>
                            <th>Name</th>
                            <th>Site</th>
                            <th>Search Term</th>
                            <th>Location</th>
                            <th>Schedule</th>
                            <th>Status</th>
                            <th>Next Run</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for worker in workers %}
                            <tr>
                                <td>
                                    <strong>{{ worker.get('name', 'N/A') }}</strong>
                                    {% if worker.get('description') %}
                                        <br><small class="text-muted">{{ worker.get('description') }}</small>
                                    {% endif %}
                                </td>
                                <td>
                                    <span class="badge bg-info">{{ worker.get('site', 'N/A').title() }}</span>
                                </td>
                                <td>{{ worker.get('search_term', 'N/A') or 'All' }}</td>
                                <td>{{ worker.get('location', 'N/A') or 'All' }}</td>
                                <td>
                                    <small class="text-muted">
                                        Every {{ worker.get('schedule_hours', 24) }}h
                                        {% if worker.get('schedule_minute_offset', 0) > 0 %}
                                            +{{ worker.get('schedule_minute_offset', 0) }}m
                                        {% endif %}
                                    </small>
                                </td>
                                <td>
                                    <span class="badge bg-{{ 'success' if worker.get('status') == 'active' else 'warning' }}">
                                        {{ worker.get('status', 'N/A') }}
                                    </span>
                                </td>
                                <td>
                                    {% if worker.get('next_run') %}
                                        {{ worker.get('next_run').strftime('%H:%M %d/%m') }}
                                    {% else %}
                                        <span class="text-muted">-</span>
                                    {% endif %}
                                </td>
                                <td>
                                    <div class="btn-group btn-group-sm" role="group">
                                        <a href="{{ url_for('edit_worker', worker_id=worker.get('id')) }}" 
                                           class="btn btn-outline-primary" title="Edit">
                                            <i class="bi bi-pencil"></i>
                                        </a>
                                        <a href="{{ url_for('execute_worker', worker_id=worker.get('id')) }}"
                                           class="btn btn-outline-success" title="Execute"
                                           onclick="return confirm('Execute worker now?')">
                                            <i class="bi bi-play-fill"></i>
                                        </a>
                                        <a href="{{ url_for('toggle_worker', worker_id=worker.get('id')) }}"
                                           class="btn btn-outline-warning" title="Toggle">
                                            <i class="bi bi-pause-fill" id="pause-{{ worker.get('id') }}"></i>
                                        </a>
                                        <a href="{{ url_for('delete_worker', worker_id=worker.get('id')) }}"
                                           class="btn btn-outline-danger" title="Delete"
                                           onclick="return confirm('Delete worker permanently?')">
                                            <i class="bi bi-trash"></i>
                                        </a>
                                    </div>
                                    <button class="btn btn-sm btn-outline-info mt-1" 
                                            onclick="loadWorkerHistory({{ worker.get('id') }})"
                                            title="History">
                                        <i class="bi bi-clock-history"></i>
                                    </button>
                                </td>
                            </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        {% else %}
            <div class="text-center py-5">
                <i class="bi bi-robot" style="font-size: 4rem; color: #6c757d;"></i>
                <h3 class="mt-3 text-muted">No Workers Configured</h3>
                <p class="text-muted">Create your first worker to start scraping jobs</p>
                <a href="{{ url_for('new_worker') }}" class="btn btn-primary">
                    <i class="bi bi-plus-lg"></i> Create Worker
                </a>
            </div>
        {% endif %}
    </div>
</div>

<!-- Worker History Modal -->
<div class="modal fade" id="historyModal" tabindex="-1">
    <div class="modal-dialog modal-lg">
        <div class="modal-content">
            <div class="modal-header">
                <h5 class="modal-title">Worker Execution History</h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
            </div>
            <div class="modal-body">
                <div id="historyContent">
                    <div class="text-center">
                        <div class="spinner-border" role="status">
                            <span class="visually-hidden">Loading...</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>
{% endblock %}

{% block scripts %}
<script>
function loadWorkerHistory(workerId) {
    const modal = new bootstrap.Modal(document.getElementById('historyModal'));
    modal.show();
    
    document.getElementById('historyContent').innerHTML = `
        <div class="text-center">
            <div class="spinner-border" role="status">
                <span class="visually-hidden">Loading...</span>
            </div>
        </div>
    `;
    
    fetch(`/api/workers/${workerId}/history`)
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                let html = '';
                if (data.data.length > 0) {
                    html = `
                        <div class="table-responsive">
                            <table class="table table-sm">
                                <thead>
                                    <tr>
                                        <th>Time</th>
                                        <th>Status</th>
                                        <th>Jobs Found</th>
                                        <th>Duration</th>
                                        <th>Error</th>
                                    </tr>
                                </thead>
                                <tbody>
                    `;
                    data.data.forEach(exec => {
                        html += `
                            <tr>
                                <td>${new Date(exec.execution_start).toLocaleString()}</td>
                                <td>
                                    <span class="badge bg-${exec.status === 'success' ? 'success' : 'danger'}">
                                        ${exec.status}
                                    </span>
                                </td>
                                <td>${exec.jobs_found || 0}</td>
                                <td>${exec.duration_seconds || 0}s</td>
                                <td>${exec.error_message || '-'}</td>
                            </tr>
                        `;
                    });
                    html += '</tbody></table></div>';
                } else {
                    html = '<div class="text-muted text-center py-3">No execution history found</div>';
                }
                document.getElementById('historyContent').innerHTML = html;
            } else {
                document.getElementById('historyContent').innerHTML = `
                    <div class="alert alert-danger">
                        Error loading history: ${data.error}
                    </div>
                `;
            }
        })
        .catch(error => {
            console.error('Error:', error);
            document.getElementById('historyContent').innerHTML = `
                <div class="alert alert-danger">
                    Error loading history data
                </div>
            `;
        });
}
</script>
{% endblock %}
EOF

    # Create databases template (simple version)
    cat > templates/databases.html << 'EOF'
{% extends "base.html" %}

{% block title %}Databases - Job Scraper Admin{% endblock %}

{% block content %}
<div class="d-flex justify-content-between align-items-center mb-4">
    <h1 class="mb-0">Databases</h1>
    <a href="{{ url_for('new_database') }}" class="btn btn-primary">
        <i class="bi bi-database-add"></i> Add Database
    </a>
</div>

<div class="card">
    <div class="card-body">
        {% if databases %}
            <div class="table-responsive">
                <table class="table table-striped table-hover">
                    <thead>
                        <tr>
                            <th>Name</th>
                            <th>Host</th>
                            <th>Database</th>
                            <th>Status</th>
                            <th>Workers</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for db in databases %}
                            <tr>
                                <td>
                                    <strong>{{ db.get('name', 'N/A') }}</strong>
                                    {% if db.get('description') %}
                                        <br><small class="text-muted">{{ db.get('description') }}</small>
                                    {% endif %}
                                </td>
                                <td>{{ db.get('host', 'N/A') }}:{{ db.get('port', 'N/A') }}</td>
                                <td>{{ db.get('database_name', 'N/A') }}</td>
                                <td>
                                    <span class="badge bg-{{ 
                                        'success' if db.get('connection_status') == 'success' 
                                        else 'danger' if db.get('connection_status') == 'failed' 
                                        else 'warning' 
                                    }}">
                                        {{ db.get('connection_status', 'untested')|title }}
                                    </span>
                                </td>
                                <td>{{ db.get('worker_count', 0) }}</td>
                                <td>
                                    <div class="btn-group btn-group-sm" role="group">
                                        <button class="btn btn-outline-primary" 
                                                onclick="testDatabase({{ db.get('id') }})"
                                                title="Test Connection">
                                            <i class="bi bi-plug"></i>
                                        </button>
                                        <a href="{{ url_for('edit_database', database_id=db.get('id')) }}" 
                                           class="btn btn-outline-secondary" title="Edit">
                                            <i class="bi bi-pencil"></i>
                                        </a>
                                    </div>
                                </td>
                            </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        {% else %}
            <div class="text-center py-5">
                <i class="bi bi-database-x" style="font-size: 4rem; color: #6c757d;"></i>
                <h3 class="mt-3 text-muted">No Databases Configured</h3>
                <p class="text-muted">Add your first database to store scraped jobs</p>
                <a href="{{ url_for('new_database') }}" class="btn btn-primary">
                    <i class="bi bi-database-add"></i> Add Database
                </a>
            </div>
        {% endif %}
    </div>
</div>
{% endblock %}

{% block scripts %}
<script>
function testDatabase(dbId) {
    const button = event.target.closest('button');
    const originalHtml = button.innerHTML;
    
    button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status"></span>';
    button.disabled = true;
    
    fetch(`/api/databases/${dbId}/test`)
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                button.innerHTML = '<i class="bi bi-check-circle-fill text-success"></i>';
                setTimeout(() => {
                    button.innerHTML = originalHtml;
                    button.disabled = false;
                    location.reload();
                }, 2000);
            } else {
                button.innerHTML = '<i class="bi bi-x-circle-fill text-danger"></i>';
                setTimeout(() => {
                    button.innerHTML = originalHtml;
                    button.disabled = false;
                }, 2000);
            }
        })
        .catch(error => {
            console.error('Error:', error);
            button.innerHTML = '<i class="bi bi-exclamation-triangle-fill text-warning"></i>';
            setTimeout(() => {
                button.innerHTML = originalHtml;
                button.disabled = false;
            }, 2000);
        });
}
</script>
{% endblock %}
EOF

    # Create error template
    cat > templates/error.html << 'EOF'
{% extends "base.html" %}

{% block title %}Error - Job Scraper Admin{% endblock %}

{% block content %}
<div class="row justify-content-center">
    <div class="col-md-8 col-lg-6">
        <div class="card">
            <div class="card-body text-center">
                <i class="bi bi-exclamation-triangle-fill text-danger" style="font-size: 5rem;"></i>
                <h3 class="mt-3">Something went wrong</h3>
                <p class="text-muted">{{ error }}</p>
                <div class="mt-4">
                    <a href="{{ url_for('dashboard') }}" class="btn btn-primary">
                        <i class="bi bi-house-door"></i> Back to Dashboard
                    </a>
                </div>
            </div>
        </div>
    </div>
</div>
{% endblock %}
EOF

    log "✓ HTML templates created"
}

# Create CSS styles
create_css() {
    log "Creating CSS styles..."
    
    cat > static/css/style.css << 'EOF'
body {
    background-color: #f8f9fa;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
}

.navbar-brand {
    font-weight: bold;
    font-size: 1.25rem;
}

.card {
    border: 1px solid rgba(0, 0, 0, 0.125);
    box-shadow: 0 0.125rem 0.25rem rgba(0, 0, 0, 0.075);
    border-radius: 0.375rem;
}

.card-header {
    background-color: #f8f9fa;
    border-bottom: 1px solid rgba(0, 0, 0, 0.125);
    font-weight: 600;
}

.btn-group-sm .btn {
    padding: 0.25rem 0.5rem;
    font-size: 0.875rem;
}

.badge {
    font-weight: 500;
    text-transform: uppercase;
    font-size: 0.675rem;
    letter-spacing: 0.025em;
}

.table {
    background-color: white;
}

.table thead th {
    border-bottom: 2px solid #dee2e6;
    background-color: #f8f9fa;
}

.table-hover tbody tr:hover {
    background-color: rgba(0, 0, 0, 0.04);
}

.alert {
    border-radius: 0.375rem;
    border: 1px solid transparent;
}

.modal-content {
    border-radius: 0.5rem;
}

.modal-header {
    border-bottom: 1px solid #dee2e6;
    background-color: #f8f9fa;
}

.modal-footer {
    border-top: 1px solid #dee2e6;
    background-color: #f8f9fa;
}

.spinner-border {
    width: 3rem;
    height: 3rem;
}

.spinner-border-sm {
    width: 1rem;
    height: 1rem;
}

.text-center .text-muted {
    color: #6c757d !important;
}

.footer {
    margin-top: 50px;
    padding: 20px 0;
    background-color: #343a40;
    color: white;
    text-align: center;
}

/* Custom status badges */
.status-active { color: #198754; }
.status-paused { color: #fd7e14; }
.status-error { color: #dc3545; }

/* Worker status indicators */
.worker-status-active::before { content: '●'; color: #198754; }
.worker-status-paused::before { content: '●'; color: #fd7e14; }
.worker-status-error::before { content: '●'; color: #dc3545; }

/* Database health indicators */
.database-health-healthy::before { content: '●'; color: #198754; }
.database-health-warning::before { content: '●'; color: #fd7e14; }
.database-health-error::before { content: '●'; color: #dc3545; }

/* Responsive adjustments */
@media (max-width: 768px) {
    .card {
        margin-bottom: 1rem;
    }
    
    .table-responsive {
        border-radius: 0.375rem;
    }
    
    .btn-group {
        flex-direction: column;
        width: 100%;
    }
    
    .btn-group .btn {
        border-radius: 0.375rem !important;
        margin-bottom: 0.25rem;
    }
}

/* Loading animation */
.loading {
    display: inline-block;
    width: 20px;
    height: 20px;
    border: 3px solid rgba(255,255,255,.3);
    border-radius: 50%;
    border-top-color: #fff;
    animation: spin 1s ease-in-out infinite;
}

@keyframes spin {
    to { transform: rotate(360deg); }
}

/* Success/Error animations */
@keyframes fadeInOut {
    0% { opacity: 0; }
    10% { opacity: 1; }
    90% { opacity: 1; }
    100% { opacity: 0; }
}

.fade-animation {
    animation: fadeInOut 3s ease-in-out;
}
EOF

    log "✓ CSS styles created"
}

# Create JavaScript
create_js() {
    log "Creating JavaScript..."
    
    cat > static/js/app.js << 'EOF'
// Main application JavaScript

document.addEventListener('DOMContentLoaded', function() {
    // Initialize tooltips
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    var tooltipList = tooltipTriggerList.map(function(tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // Initialize popovers
    var popoverTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="popover"]'));
    var popoverList = popoverTriggerList.map(function(popoverTriggerEl) {
        return new bootstrap.Popover(popoverTriggerEl);
    });

    // Auto-refresh functionality
    setupAutoRefresh();
    
    // Setup global error handling
    setupErrorHandling();
});

// Auto-refresh functionality
function setupAutoRefresh() {
    const refreshInterval = 30000; // 30 seconds
    
    setInterval(() => {
        if (window.location.pathname === '/') {
            refreshDashboard();
        }
    }, refreshInterval);
}

// Refresh dashboard data
function refreshDashboard() {
    const refreshButton = document.querySelector('[onclick="location.reload()"]');
    if (refreshButton) {
        refreshButton.click();
    } else {
        location.reload();
    }
}

// Global error handling
function setupErrorHandling() {
    window.addEventListener('error', function(e) {
        console.error('Global error:', e.error);
        showNotification('An unexpected error occurred', 'danger');
    });

    window.addEventListener('unhandledrejection', function(e) {
        console.error('Unhandled promise rejection:', e.reason);
        showNotification('A network error occurred', 'warning');
    });
}

// Notification system
function showNotification(message, type = 'info') {
    const notification = document.createElement('div');
    notification.className = `alert alert-${type} alert-dismissible fade show`;
    notification.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    
    // Insert at the top of the container
    const container = document.querySelector('.container');
    if (container) {
        container.parentNode.insertBefore(notification, container);
        
        // Auto-remove after 5 seconds
        setTimeout(() => {
            if (notification.parentNode) {
                notification.remove();
            }
        }, 5000);
    }
}

// Worker management functions
function toggleWorker(workerId) {
    if (!confirm('Are you sure you want to toggle this worker?')) {
        return;
    }
    
    const button = event.target.closest('button');
    const originalHtml = button.innerHTML;
    
    button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status"></span>';
    button.disabled = true;
    
    window.location.href = `/workers/${workerId}/toggle`;
}

function deleteWorker(workerId) {
    if (!confirm('Are you sure you want to delete this worker permanently?')) {
        return;
    }
    
    window.location.href = `/workers/${workerId}/delete`;
}

function executeWorker(workerId) {
    if (!confirm('Execute worker manually? This will start an immediate scraping run.')) {
        return;
    }
    
    const button = event.target.closest('button');
    const originalHtml = button.innerHTML;
    
    button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status"></span>';
    button.disabled = true;
    
    window.location.href = `/workers/${workerId}/execute`;
}

// Database management functions
function testDatabase(dbId) {
    const button = event.target.closest('button');
    const originalHtml = button.innerHTML;
    
    button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status"></span>';
    button.disabled = true;
    
    fetch(`/api/databases/${dbId}/test`)
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                button.innerHTML = '<i class="bi bi-check-circle-fill text-success"></i>';
                showNotification('Database connection successful', 'success');
            } else {
                button.innerHTML = '<i class="bi bi-x-circle-fill text-danger"></i>';
                showNotification('Database connection failed: ' + data.error, 'danger');
            }
            
            setTimeout(() => {
                button.innerHTML = originalHtml;
                button.disabled = false;
            }, 2000);
        })
        .catch(error => {
            console.error('Error:', error);
            button.innerHTML = '<i class="bi bi-exclamation-triangle-fill text-warning"></i>';
            showNotification('Network error while testing database', 'warning');
            
            setTimeout(() => {
                button.innerHTML = originalHtml;
                button.disabled = false;
            }, 2000);
        });
}

// All databases test
function testAllConnections() {
    fetch('/api/databases/test-all')
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                showNotification('All connections tested successfully', 'success');
            } else {
                showNotification('Some connections failed: ' + data.error, 'warning');
            }
        })
        .catch(error => {
            console.error('Error:', error);
            showNotification('Network error while testing connections', 'warning');
        });
}

// AJAX helper functions
function makeRequest(url, options = {}) {
    const defaultOptions = {
        headers: {
            'Content-Type': 'application/json',
        },
        credentials: 'same-origin'
    };
    
    const mergedOptions = { ...defaultOptions, ...options };
    
    return fetch(url, mergedOptions)
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        });
}

// Form validation helpers
function validateRequired(form, ...fieldNames) {
    for (const fieldName of fieldNames) {
        const field = form.elements[fieldName];
        if (!field || !field.value.trim()) {
            showNotification(`${fieldName} is required`, 'warning');
            field.focus();
            return false;
        }
    }
    return true;
}

// Copy to clipboard helper
async function copyToClipboard(text) {
    try {
        await navigator.clipboard.writeText(text);
        showNotification('Copied to clipboard!', 'success');
    } catch (err) {
        console.error('Failed to copy: ', err);
        showNotification('Failed to copy to clipboard', 'warning');
    }
}

// Format date/time helpers
function formatDateTime(dateString) {
    const date = new Date(dateString);
    return date.toLocaleString();
}

function formatTimeAgo(dateString) {
    const date = new Date(dateString);
    const now = new Date();
    const seconds = Math.floor((now - date) / 1000);
    
    if (seconds < 60) return 'just now';
    if (seconds < 3600) return Math.floor(seconds / 60) + ' minutes ago';
    if (seconds < 86400) return Math.floor(seconds / 3600) + ' hours ago';
    if (seconds < 2592000) return Math.floor(seconds / 86400) + ' days ago';
    if (seconds < 31536000) return Math.floor(seconds / 2592000) + ' months ago';
    return Math.floor(seconds / 31536000) + ' years ago';
}

// Loading states
function setLoadingState(element, loading = true) {
    if (loading) {
        element.disabled = true;
        element.dataset.originalHtml = element.innerHTML;
        element.innerHTML = '<span class="spinner-border spinner-border-sm" role="status"></span>';
    } else {
        element.disabled = false;
        element.innerHTML = element.dataset.originalHtml || element.innerHTML;
    }
}

// Utility functions
function debounce(func, wait, immediate) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            timeout = null;
            if (!immediate) func(...args);
        };
        const callNow = immediate && !timeout;
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
        if (callNow) func(...args);
    };
}

// Initialize tooltips on dynamic content
function initializeTooltips() {
    const tooltips = document.querySelectorAll('[data-bs-toggle="tooltip"]:not([data-bs-initialized])');
    tooltips.forEach(tooltip => {
        new bootstrap.Tooltip(tooltip);
        tooltip.dataset.bsInitialized = 'true';
    });
}

// Responsive helper
function isMobile() {
    return window.innerWidth <= 768;
}

// Table sort helper
function sortTable(table, column, type = 'string') {
    const tbody = table.querySelector('tbody');
    const rows = Array.from(tbody.querySelectorAll('tr'));
    const isAscending = table.dataset.sortDirection !== 'asc';
    
    rows.sort((a, b) => {
        const aValue = a.children[column].textContent.trim();
        const bValue = b.children[column].textContent.trim();
        
        if (type === 'number') {
            return isAscending ? 
                parseFloat(aValue) - parseFloat(bValue) : 
                parseFloat(bValue) - parseFloat(aValue);
        } else if (type === 'date') {
            return isAscending ?
                new Date(aValue) - new Date(bValue) :
                new Date(bValue) - new Date(aValue);
        } else {
            return isAscending ?
                aValue.localeCompare(bValue) :
                bValue.localeCompare(aValue);
        }
    });
    
    rows.forEach(row => tbody.appendChild(row));
    table.dataset.sortDirection = isAscending ? 'asc' : 'desc';
}

// Export functions for global use
window.showNotification = showNotification;
window.makeRequest = makeRequest;
window.validateRequired = validateRequired;
window.copyToClipboard = copyToClipboard;
window.formatDateTime = formatDateTime;
window.formatTimeAgo = formatTimeAgo;
window.setLoadingState = setLoadingState;
window.debounce = debounce;
window.initializeTooltips = initializeTooltips;
window.isMobile = isMobile;
window.sortTable = sortTable;
EOF

    log "✓ JavaScript created"
}

# Docker deployment
deploy_docker() {
    log "Starting Docker deployment..."
    
    # Build custom images
    log "Building Docker images..."
    docker-compose build --no-cache || {
        error "Failed to build Docker images"
        exit 1
    }
    
    # Start services (excluding external postgres/redis)
    log "Starting services..."
    docker-compose up -d --remove-orphans || {
        error "Failed to start services"
        exit 1
    }
    
    # Wait for services to be ready
    log "Waiting for services to start..."
    sleep 30
    
    # Check service health
    log "Checking service health..."
    
    # Check web admin
    if ! curl -f http://localhost:5000/health >/dev/null 2>&1; then
        warn "Web admin service not responding immediately, will continue..."
    else
        log "✓ Web admin service is responding"
    fi
    
    # Check worker scheduler
    if ! curl -f http://localhost:8080/health >/dev/null 2>&1; then
        warn "Worker scheduler not responding immediately, will continue..."
    else
        log "✓ Worker scheduler is responding"
    fi
}

# Initialize database schema
initialize_database() {
    log "Initializing database schema..."
    
    # Try multiple approaches to initialize schema
    
    # Approach 1: Use web admin container
    log "Approach 1: Initializing schema via web admin..."
    if docker-compose exec web_admin python -c "
import psycopg2
import os
from psycopg2 import sql

db_url = os.getenv('POSTGRES_URL')
if not db_url:
    print('ERROR: POSTGRES_URL not found')
    exit(1)

print('Main script...')
try:
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    
    # Read and execute schema
    with open('/app/modular_schema.sql', 'r') as f:
        schema_content = f.read()
    
    # Split into statements and execute
    statements = schema_content.split(';')
    for i, statement in enumerate(statements):
        statement = statement.strip()
        if statement and not statement.startswith('--'):
            try:
                cursor.execute(statement)
                conn.commit()
                print(f'Statement {i+1}: OK')
            except Exception as e:
                if 'already exists' not in str(e).lower() and 'duplicate' not in str(e).lower():
                    print(f'Statement {i+1}: ERROR - {e}')
                    conn.rollback()
                else:
                    conn.commit()
                    print(f'Statement {i+1}: ALREADY EXISTS')
    
    cursor.close()
    conn.close()
    print('Database schema initialized successfully')
except Exception as e:
    print(f'Database initialization error: {e}')
    exit(1)
" 2>/dev/null; then
        log "✓ Database schema initialized via web admin"
        return
    fi
    
    # Approach 2: Direct schema execution
    log "Approach 2: Direct schema execution..."
    if python3 -c "
import psycopg2
import os
from psycopg2 import sql

db_url = os.getenv('POSTGRES_URL')
if not db_url:
    print('ERROR: POSTGRES_URL not found')
    exit(1)

print('Direct execution...')
try:
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    
    # Read and execute schema
    with open('modular_schema.sql', 'r') as f:
        schema_content = f.read()
    
    # Split into statements and execute
    statements = schema_content.split(';')
    for i, statement in enumerate(statements):
        statement = statement.strip()
        if statement and not statement.startswith('--'):
            try:
                cursor.execute(statement)
                conn.commit()
                print(f'Statement {i+1}: OK')
            except Exception as e:
                if 'already exists' not in str(e).lower() and 'duplicate' not in str(e).lower():
                    print(f'Statement {i+1}: ERROR - {e}')
                    conn.rollback()
                else:
                    conn.commit()
                    print(f'Statement {i+1}: ALREADY EXISTS')
    
    cursor.close()
    conn.close()
    print('Database schema initialized successfully')
except Exception as e:
    print(f'Database initialization error: {e}')
    exit(1)
" 2>/dev/null; then
        log "✓ Database schema initialized directly"
        return
    fi
    
    warn "Database schema initialization may need manual execution"
}

# Main deployment function
main() {
    log "🚀 Job Scraper VPS Deployment Started"
    echo "===================================="
    
    # Load environment variables
    if [ -f ".env" ]; then
        export $(grep -v '^#' .env | xargs)
        log "✓ Environment variables loaded"
    else
        warn "Environment file not found, using existing environment"
    fi
    
    # Check prerequisites
    check_vps
    
    # Setup directory structure
    setup_directories
    
    # Setup SSL
    setup_ssl
    
    # Create templates and assets
    create_templates
    create_css
    create_js
    
    # Deploy Docker services
    deploy_docker
    
    # Initialize database
    initialize_database
    
    # Display success information
    log "✅ VPS Deployment Complete!"
    echo ""
    echo "🌐 Access Information:"
    echo "========================"
    echo "📊 Web Admin Interface: http://localhost:5000"
    echo "📊 Nginx Proxy: http://localhost"
    echo ""
    echo "🚀 Services Status:"
    echo "=================="
    docker-compose ps
    echo ""
    echo "📝 Next Steps:"
    echo "=============="
    echo "1. Access the web admin at http://localhost:5000"
    echo "2. Add your target databases in the Databases section"
    echo "3. Create your first worker configuration"
    echo "4. Test the worker execution"
    echo "5. Set up monitoring and alerts as needed"
    echo ""
    echo "🔧 Management Commands:"
    echo "========================"
    echo "View logs: docker-compose logs -f [service_name]"
    echo "Stop services: docker-compose down"
    echo "Restart services: docker-compose restart"
    echo "Rebuild: docker-compose build && docker-compose up -d"
    echo ""
    echo "✨ Happy Scraping!"
}

# Error handling
set -e
trap 'error "Deployment failed at line $LINENO"' ERR

# Run main function
main "$@"