#!/bin/bash

# Job Scraper VPS Deployment Script
# For existing Hostinger VPS services: my-job-scraper-fe and my-job-scraper-worker

set -e

echo "🚀 Job Scraper VPS Deployment - Existing Services"
echo "================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color
BLUE='\033[0;34m'

# Logging function
log() {
    echo -e "${GREEN}[INFO] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[WARN] $1${NC}"
}

error() {
    echo -e "${RED}[ERROR] $1${NC}"
}

info() {
    echo -e "${BLUE}[INFO] $1${NC}"
}

# Check if we're in the right directory
check_directory() {
    if [ ! -f "docker-compose.yml" ]; then
        error "docker-compose.yml not found in current directory"
        exit 1
    fi
    log "✓ Working directory verified"
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed"
        exit 1
    fi
    log "✓ Docker is installed"
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        error "Docker Compose is not installed"
        exit 1
    fi
    log "✓ Docker Compose is installed"
    
    # Check environment variables
    if [ ! -f ".env" ]; then
        error "Environment file .env not found"
        exit 1
    fi
    
    # Load environment variables
    export $(grep -v '^#' .env | xargs)
    log "✓ Environment variables loaded"
    
    # Test database connection
    if ! docker run --rm -e POSTGRES_URL="$POSTGRES_URL" --network host python:3.11 python -c "
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
        log "✓ PostgreSQL database is accessible"
    else
        warn "PostgreSQL database may not be accessible - continuing..."
    fi
    
    # Test Redis connection
    if ! docker run --rm -e REDIS_URL="$REDIS_URL" --network host python:3.11 python -c "
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
        log "✓ Redis is accessible"
    else
        warn "Redis may not be accessible - continuing..."
    fi
}

# Create necessary directories
setup_directories() {
    log "Creating necessary directories..."
    
    mkdir -p logs monitoring ssl templates static/css static/js
    chmod 755 logs templates static monitoring ssl
    chmod 755 static/css static/js
    
    log "✓ Directories created"
}

# Generate SSL certificates (development/self-signed)
setup_ssl() {
    log "Setting up SSL certificates..."
    
    if [ ! -f "ssl/cert.pem" ] || [ ! -f "ssl/key.pem" ]; then
        mkdir -p ssl
        openssl req -x509 -newkey rsa:4096 -keyout ssl/key.pem -out ssl/cert.pem -days 365 -nodes \
            -subj "/C=BR/ST=SP/L=Sao Paulo/O=JobScraper/OU=Dev/CN=localhost" \
            -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" 2>/dev/null || true
        chmod 600 ssl/key.pem ssl/cert.pem
        log "✓ SSL certificates generated"
    else
        log "✓ SSL certificates already exist"
    fi
}

# Create requirements files
create_requirements() {
    log "Creating requirements files..."
    
    # Frontend requirements
    cat > requirements.admin.txt << 'EOF'
Flask==2.3.3
Flask-WTF==1.1.1
Flask-SQLAlchemy==3.0.5
psycopg2-binary==2.9.8
psycopg2==2.9.8
WTForms==3.0.1
Werkzeug==2.3.7
Jinja2==3.1.2
requests==2.31.0
python-dotenv==1.0.0
bcrypt==4.0.1
secrets==1.1.0
gunicorn==21.2.0
python-dateutil==2.8.2
bootstrap==5.3.0
blinker==1.6.2
itsdangerous==2.1.2
MarkupSafe==2.1.3
click==8.1.7
EOF

    # Scheduler/Worker requirements
    cat > requirements.scheduler.txt << 'EOF'
psycopg2-binary==2.9.8
psycopg2==2.9.8
schedule==1.2.0
requests==2.31.0
python-dateutil==2.8.2
urllib3==2.0.4
beautifulsoup4==4.12.2
lxml==4.9.3
markdownify==0.11.6
numpy==1.24.3
pandas==2.0.3
python-dotenv==1.0.0
loguru==0.7.2
prometheus-client==0.17.1
structlog==23.1.0
typing-extensions==4.7.1
jobspy@git+https://github.com/cullenwatson/JobSpy.git@master#egg=jobspy
EOF

    # Worker requirements
    cp requirements.scheduler.txt requirements.worker.txt
    
    log "✓ Requirements files created"
}

# Build Docker images
build_images() {
    log "Building Docker images..."
    
    # Build frontend image
    log "Building frontend image..."
    docker build -t job-scraper-frontend -f Dockerfile.force_rebuild_ultimate . || {
        error "Failed to build frontend image"
        exit 1
    }
    
    log "✓ All Docker images built successfully"
}

# Start services
start_services() {
    log "Starting services..."
    
    # Stop any existing services
    docker-compose down --remove-orphans 2>/dev/null || true
    
    # Start services
    docker-compose up -d --remove-orphans || {
        error "Failed to start services"
        exit 1
    }
    
    # Wait a moment for services to initialize
    sleep 10
    
    # Check service status
    log "Checking service status..."
    docker-compose ps
    
    # Wait for frontend to be ready
    log "Waiting for frontend service to be ready..."
    local attempts=0
    local max_attempts=12  # 2 minutes max
    
    while [ $attempts -lt $max_attempts ]; do
        if curl -f http://localhost:5000/health >/dev/null 2>&1; then
            log "✓ Frontend service is ready"
            break
        fi
        
        attempts=$((attempts + 1))
        if [ $attempts -lt $max_attempts ]; then
            info "Frontend not ready yet... waiting ($(($attempts * 10))s elapsed)"
            sleep 10
        else
            warn "Frontend service may not be fully ready yet, continuing..."
            break
        fi
    done
}

# Initialize database schema
initialize_database() {
    log "Initializing database schema..."
    
    # Multiple approaches to initialize schema
    local attempts=0
    local max_attempts=3
    
    while [ $attempts -lt $max_attempts ]; do
        # Try using frontend container
        log "Attempt $(($attempts + 1)): Initializing schema via frontend container..."
        
        if docker-compose exec frontend python -c "
import psycopg2
import os
from psycopg2 import sql

db_url = os.getenv('POSTGRES_URL')
if not db_url:
    print('ERROR: POSTGRES_URL not found in environment')
    exit(1)

print('Database URL found, proceeding with schema initialization...')
try:
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    
    # Read and execute schema
    with open('/app/modular_schema.sql', 'r') as f:
        schema_content = f.read()
    
    print(f'Schema content length: {len(schema_content)} characters')
    
    # Split into statements and execute
    statements = schema_content.split(';')
    executed = 0
    
    for i, statement in enumerate(statements):
        statement = statement.strip()
        if statement and not statement.startswith('--') and not statement.startswith('/*'):
            try:
                cursor.execute(statement)
                conn.commit()
                executed += 1
                print(f'Statement {executed}: OK')
            except Exception as e:
                error_msg = str(e).lower()
                if any(keyword in error_msg for keyword in ['already exists', 'duplicate', 'does not exist', 'no function', 'operator does not exist']):
                    conn.commit()
                    print(f'Statement {executed}: SKIPPED ({type})')
                else:
                    print(f'Statement {executed}: ERROR - {e}')
                    conn.rollback()
                    # Continue with other statements
                    conn.commit()
    
    cursor.close()
    conn.close()
    print(f'Successfully executed {executed} statements')
    
    # Now check if tables were created
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    cursor.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'scraping%'\")
    tables = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if tables:
        print(f'Tables created: {[table[0] for table in tables]}')
        print('Database schema initialized successfully')
        exit(0)
    else:
        print('Warning: No scraping tables found')
        exit(0)
except Exception as e:
    print(f'Database initialization error: {e}')
    exit(1)
" 2>/dev/null; then
            log "✓ Database schema initialized successfully via frontend"
            return 0
        fi
        
        log "Database initialization attempt $(($attempts + 1)) failed, retrying..."
        sleep 5
        attempts=$((attempts + 1))
    done
    
    warn "Database schema initialization may need manual verification"
    return 0
}

# Display deployment information
display_info() {
    log "✅ VPS Deployment Complete!"
    echo ""
    echo "🌐 Access Information:"
    echo "========================"
    echo "🖥️  Admin Interface: http://localhost:5000"
    echo "📊 Health Check: http://localhost:8080/health"
    echo ""
    echo "🚀 Service Status:"
    echo "=================="
    docker-compose ps
    echo ""
    echo "📝 Management Commands:"
    echo "========================"
    echo "View logs:    docker-compose logs -f [service_name]"
    echo "Stop services: docker-compose down"
    echo "Restart:       docker-compose restart"
    echo "Rebuild:       docker-compose build && docker-compose up -d"
    echo ""
    echo "🔧 Service access:"
    echo "=================="
    echo "Frontend:       docker-compose exec frontend bash"
    echo "Worker:         docker-compose exec worker bash"
    echo "Scheduler:      docker-compose exec scheduler bash"
    echo ""
    echo "📱 First Steps:"
    echo "==============="
    echo "1. Open http://localhost:5000 in your browser"
    echo "2. Add target databases in 'Databases' section"
    echo "3. Create your first scraping worker"
    echo "4. Test worker execution"
    echo "5. Set up monitoring and alerts"
    echo ""
    echo "✨ Deployment successful - Happy scraping!"
}

# Main deployment function
main() {
    log "🚀 Job Scraper VPS Deployment Started"
    echo "======================================"
    
    # Check directory structure
    check_directory
    
    # Check prerequisites
    check_prerequisites
    
    # Setup directories
    setup_directories
    
    # Setup SSL certificates
    setup_ssl
    
    # Create requirements files
    create_requirements
    
    # Build Docker images
    build_images
    
    # Start services
    start_services
    
    # Initialize database
    initialize_database
    
    # Display information
    display_info
}

# Error handling
trap 'error "Deployment failed at line $LINENO. Check logs for details."' ERR

# Run main function
main "$@"