#!/bin/bash

# Job Scraper - Deployment Script
# This script sets up and deploys the modular job scraping system

set -e

echo "🚀 Starting Job Scraper Deployment..."
echo "=================================="

# Environment variables setup
export $(cat .env | xargs)

# Function to check if Docker is installed
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        echo "❌ Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    echo "✅ Docker and Docker Compose are installed"
}

# Function to create necessary directories
create_directories() {
    echo "📁 Creating necessary directories..."
    
    mkdir -p logs
    mkdir -p monitoring/grafana/dashboards
    mkdir -p monitoring/grafana/provisioning/dashboards
    mkdir -p monitoring/grafana/provisioning/datasources
    mkdir -p ssl
    mkdir -p templates
    mkdir -p static/css
    mkdir -p static/js
    
    echo "✅ Directories created"
}

# Function to generate SSL certificates (self-signed for development)
generate_ssl_certs() {
    echo "🔐 Generating SSL certificates..."
    
    if [ ! -f ssl/cert.pem ] || [ ! -f ssl/key.pem ]; then
        openssl req -x509 -newkey rsa:4096 -keyout ssl/key.pem -out ssl/cert.pem -days 365 -nodes \
            -subj "/C=BR/ST=SP/L=Sao Paulo/O=JobScraper/OU=Dev/CN=localhost" 2>/dev/null || true
        echo "✅ Self-signed SSL certificates generated"
    else
        echo "✅ SSL certificates already exist"
    fi
}

# Function to setup monitoring configuration
setup_monitoring() {
    echo "📊 Setting up monitoring configuration..."
    
    # Create monitoring/prometheus.yml if it doesn't exist
    if [ ! -f monitoring/prometheus.yml ]; then
        mkdir -p monitoring
        cat > monitoring/prometheus.yml << EOF
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  # - "first_rules.yml"
  # - "second_rules.yml"

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'nginx'
    static_configs:
      - targets: ['nginx:80']

  - job_name: 'web_admin'
    static_configs:
      - targets: ['web_admin:5000']

  - job_name: 'worker_scheduler'
    static_configs:
      - targets: ['worker_scheduler:8080']

  - job_name: 'worker_engine'
    static_configs:
      - targets: ['worker_engine_1:8080', 'worker_engine_2:8080']
EOF
        echo "✅ Prometheus configuration created"
    fi
    
    # Create Grafana datasource configuration if it doesn't exist
    if [ ! -f monitoring/grafana/provisioning/datasources/prometheus.yml ]; then
        mkdir -p monitoring/grafana/provisioning/datasources
        cat > monitoring/grafana/provisioning/datasources/prometheus.yml << EOF
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    orgId: 1
    url: http://prometheus:9090
    basicAuth: false
    isDefault: true
    version: 1
    editable: false
EOF
        echo "✅ Grafana datasource configuration created"
    fi
}

# Function to create basic HTML templates
create_templates() {
    echo "🎨 Creating basic HTML templates..."
    
    # Create base template
    cat > templates/base.html << EOF
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
            <a class="navbar-brand" href="/">
                <i class="bi bi-briefcase-fill"></i> Job Scraper
            </a>
            <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav">
                <span class="navbar-toggler-icon"></span>
            </button>
            <div class="collapse navbar-collapse" id="navbarNav">
                <ul class="navbar-nav">
                    <li class="nav-item">
                        <a class="nav-link" href="/">Dashboard</a>
                    </li>
                    <li class="nav-item">
                        <a class="nav-link" href="/workers">Workers</a>
                    </li>
                    <li class="nav-item">
                        <a class="nav-link" href="/databases">Databases</a>
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
    cat > templates/dashboard.html << EOF
{% extends "base.html" %}

{% block title %}Dashboard - Job Scraper Admin{% endblock %}

{% block content %}
<div class="row">
    <div class="col-md-3">
        <div class="card">
            <div class="card-body text-center">
                <h2 class="text-primary">{{ stats.total_workers or 0 }}</h2>
                <p class="text-muted">Total Workers</p>
            </div>
        </div>
    </div>
    <div class="col-md-3">
        <div class="card">
            <div class="card-body text-center">
                <h2 class="text-success">{{ stats.active_workers or 0 }}</h2>
                <p class="text-muted">Active Workers</p>
            </div>
        </div>
    </div>
    <div class="col-md-3">
        <div class="card">
            <div class="card-body text-center">
                <h2 class="text-warning">{{ stats.paused_workers or 0 }}</h2>
                <p class="text-muted">Paused Workers</p>
            </div>
        </div>
    </div>
    <div class="col-md-3">
        <div class="card">
            <div class="card-body text-center">
                <h2 class="text-info">{{ stats.workers_today or 0 }}</h2>
                <p class="text-muted">Workers Today</p>
            </div>
        </div>
    </div>
</div>

<div class="row mt-4">
    <div class="col-md-6">
        <div class="card">
            <div class="card-header">
                <h5>Recent Executions</h5>
            </div>
            <div class="card-body">
                {% if recent_executions %}
                    <div class="table-responsive">
                        <table class="table table-sm">
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
                                        <td>{{ exec.worker_name }}</td>
                                        <td>
                                            <span class="badge bg-{% if exec.status == 'success' %}success{% elif exec.status == 'failed' %}danger{% else %}warning{% endif %}">
                                                {{ exec.status }}
                                            </span>
                                        </td>
                                        <td>{{ exec.execution_start.strftime('%H:%M %d/%m') if exec.execution_start else 'N/A' }}</td>
                                        <td>{{ exec.jobs_found }}</td>
                                    </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                {% else %}
                    <p class="text-muted">No recent executions</p>
                {% endif %}
            </div>
        </div>
    </div>
    
    <div class="col-md-6">
        <div class="card">
            <div class="card-header">
                <h5>Scheduled Runs</h5>
            </div>
            <div class="card-body">
                {% if scheduled_runs %}
                    <div class="table-responsive">
                        <table class="table table-sm">
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
                                        <td>{{ run.name }}</td>
                                        <td>{{ run.site }}</td>
                                        <td>{{ run.next_run.strftime('%H:%M %d/%m') if run.next_run else 'N/A' }}</td>
                                        <td>{{ run.database_name }}</td>
                                    </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                {% else %}
                    <p class="text-muted">No scheduled runs</p>
                {% endif %}
            </div>
        </div>
    </div>
</div>
{% endblock %}
EOF

    echo "✅ Basic HTML templates created"
}

# Function to create basic CSS
create_css() {
    echo "🎨 Creating CSS styles..."
    
    cat > static/css/style.css << EOF
body {
    background-color: #f8f9fa;
}

.navbar-brand {
    font-weight: bold;
}

.card {
    box-shadow: 0 0.125rem 0.25rem rgba(0, 0, 0, 0.075);
    border: 1px solid rgba(0, 0, 0, 0.125);
}

.card-header {
    background-color: #f8f9fa;
    border-bottom: 1px solid rgba(0, 0, 0, 0.125);
}

.alert {
    border-radius: 0.375rem;
}

.table {
    background-color: white;
}

.badge {
    font-size: 0.75em;
}

.btn-sm {
    padding: 0.25rem 0.5rem;
    font-size: 0.875rem;
}

.worker-status-active {
    color: #198754;
}

.worker-status-paused {
    color: #fd7e14;
}

.worker-status-error {
    color: #dc3545;
}

.database-health-healthy {
    color: #198754;
}

.database-health-warning {
    color: #fd7e14;
}

.database-health-error {
    color: #dc3545;
}

.footer {
    margin-top: 50px;
    padding: 20px 0;
    background-color: #343a40;
    color: white;
    text-align: center;
}

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
EOF

    echo "✅ CSS styles created"
}

# Function to create basic JavaScript
create_js() {
    echo "📜 Creating JavaScript..."
    
    cat > static/js/app.js << EOF
// Main application JavaScript

document.addEventListener('DOMContentLoaded', function() {
    // Initialize tooltips
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    var tooltipList = tooltipTriggerList.map(function(tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
});

// Worker management functions
function toggleWorker(workerId) {
    if (confirm('Are you sure you want to toggle this worker?')) {
        window.location.href = \`/workers/\${workerId}/toggle\`;
    }
}

function deleteWorker(workerId) {
    if (confirm('Are you sure you want to delete this worker? This action cannot be undone.')) {
        window.location.href = \`/workers/\${workerId}/delete\`;
    }
}

function executeWorker(workerId) {
    if (confirm('Are you sure you want to execute this worker manually?')) {
        window.location.href = \`/workers/\${workerId}/execute\`;
    }
}

// Load worker history
function loadWorkerHistory(workerId) {
    fetch(\`/api/workers/\${workerId}/history\`)
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                // Update history display
                console.log('Worker history:', data.data);
            } else {
                console.error('Failed to load worker history:', data.error);
            }
        })
        .catch(error => console.error('Error:', error));
}

// Load worker performance
function loadWorkerPerformance(workerId) {
    fetch(\`/api/workers/\${workerId}/performance\`)
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                // Update performance display
                console.log('Worker performance:', data.data);
            } else {
                console.error('Failed to load worker performance:', data.error);
            }
        })
        .catch(error => console.error('Error:', error));
}

// Database connection test
function testDatabase(databaseId) {
    const button = document.getElementById(\`test-db-\${databaseId}\`);
    const originalText = button.textContent;
    
    button.textContent = 'Testing...';
    button.disabled = true;
    
    fetch(\`/api/databases/\${databaseId}/test\`)
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                button.textContent = '✓ Connected';
                button.className = 'btn btn-sm btn-success';
                setTimeout(() => {
                    button.textContent = originalText;
                    button.className = 'btn btn-sm btn-outline-primary';
                    button.disabled = false;
                }, 3000);
            } else {
                button.textContent = '✗ Failed';
                button.className = 'btn btn-sm btn-danger';
                setTimeout(() => {
                    button.textContent = originalText;
                    button.className = 'btn btn-sm btn-outline-primary';
                    button.disabled = false;
                }, 3000);
            }
        })
        .catch(error => {
            console.error('Error:', error);
            button.textContent = '✗ Error';
            button.className = 'btn btn-sm btn-danger';
            setTimeout(() => {
                button.textContent = originalText;
                button.className = 'btn btn-sm btn-outline-primary';
                button.disabled = false;
            }, 3000);
        });
}

// Auto-refresh dashboard (optional)
function startAutoRefresh() {
    setInterval(() => {
        if (window.location.pathname === '/') {
            window.location.reload();
        }
    }, 30000); // Refresh every 30 seconds
}

// Initialize auto-refresh for dashboard
if (window.location.pathname === '/') {
    startAutoRefresh();
}

// Form validation helpers
function validateWorkerForm(form) {
    const name = form.querySelector('[name="name"]').value;
    const database = form.querySelector('[name="database_id"]').value;
    const site = form.querySelector('[name="site"]').value;
    
    if (!name || !database || !site) {
        alert('Please fill in all required fields.');
        return false;
    }
    
    return true;
}

// Copy to clipboard helper
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(() => {
        alert('Copied to clipboard!');
    }).catch(err => {
        console.error('Failed to copy:', err);
    });
}
EOF

    echo "✅ JavaScript created"
}

# Function to setup and deploy
setup_and_deploy() {
    echo "🔧 Setting up and deploying services..."
    
    # Pull latest images
    echo "📦 Pulling Docker images..."
    docker-compose pull
    
    # Build custom images
    echo "🏗️  Building custom Docker images..."
    docker-compose build --no-cache
    
    # Start services
    echo "🚀 Starting services..."
    docker-compose up -d
    
    # Wait for services to start
    echo "⏳ Waiting for services to start..."
    sleep 30
    
    # Run database schema initialization
    echo "🗄️  Initializing database schema..."
    docker-compose exec web_admin python -c "
import psycopg2
import os
from psycopg2 import sql

# Get database URL from environment
db_url = os.getenv('POSTGRES_URL')
if not db_url:
    print('No POSTGRES_URL found')
    exit(1)

# Connect to database and run schema
try:
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    
    # Read and execute schema
    with open('/app/modular_schema.sql', 'r') as f:
        schema_content = f.read()
    
    # Split into statements and execute
    statements = schema_content.split(';')
    for statement in statements:
        statement = statement.strip()
        if statement and not statement.startswith('--'):
            try:
                cursor.execute(statement)
                conn.commit()
            except Exception as e:
                if 'already exists' not in str(e).lower() and 'duplicate' not in str(e).lower():
                    print(f'Schema error: {e}')
                    conn.rollback()
                else:
                    conn.commit()
    
    cursor.close()
    conn.close()
    print('Database schema initialized successfully')
except Exception as e:
    print(f'Database initialization error: {e}')
    exit(1)
"
    
    echo "✅ Database schema initialized"
}

# Function to show access information
show_access_info() {
    echo "🌐 Access Information:"
    echo "========================"
    echo "📊 Web Admin Interface: https://localhost"
    echo "📊 Grafana Dashboard: http://localhost:3000 (admin/admin)"
    echo "📊 Prometheus Metrics: http://localhost:9090"
    echo ""
    echo "🔑 To get the container logs, run:"
    echo "   docker-compose logs -f [service_name]"
    echo ""
    echo "🛑 To stop the services, run:"
    echo "   docker-compose down"
    echo ""
    echo "🔄 To restart the services, run:"
    echo "   docker-compose restart"
    echo ""
    echo "✅ Deployment complete!"
}

# Main deployment function
main() {
    echo "🚀 Job Scraper Deployment Script"
    echo "================================"
    
    # Check prerequisites
    check_docker
    
    # Create directory structure
    create_directories
    
    # Generate SSL certificates
    generate_ssl_certs
    
    # Setup monitoring configuration
    setup_monitoring
    
    # Create templates and assets
    create_templates
    create_css
    create_js
    
    # Setup and deploy
    setup_and_deploy
    
    # Show access information
    show_access_info
}

# Run main function
main "$@"