# EasyPanel Job Scraper Deployment

## Overview
This guide shows how to deploy the Job Scraper system using EasyPanel - a much simpler approach than complex scripts!

## Prerequisites
- EasyPanel installed on your Hostinger VPS
- PostgreSQL and Redis already running (as you mentioned)
- JobSpy codebase uploaded to your server

## EasyPanel Deployment Steps

### Method 1: Docker Compose (Recommended)

#### Frontend Service (my-job-scraper-fe)
1. Click "Add Service" → "Docker Compose" → "Upload Files"
2. Service Name: `job-scraper-frontend`
3. Docker Compose Configuration:
```yaml
version: '3.8'
services:
  frontend:
    build:
      context: .
      dockerfile: Dockerfile.force_rebuild_ultimate
    container_name: my-job-scraper-fe
    restart: unless-stopped
    environment:
      POSTGRES_URL: postgres://aromanon:Afmg2486!@my-job-scraper_my-job-scraper:5432/job-data?sslmode=disable
      REDIS_URL: redis://default:Afmg2486!@my-job-scraper_my-job-scraper_redis:6379
      SECRET_KEY: your-secret-key-here
      FLASK_APP: worker_admin.py
      PYTHONPATH: /app:/app/jobspy
    ports:
      - "5000:5000"
    volumes:
      - ./logs:/app/logs
      - ./templates:/app/templates
      - ./static:/app/static
    healthcheck:
      test: ["CMD", "python", "-c", "import requests; requests.get('http://localhost:5000/health')"]
      interval: 30s
      timeout: 10s
      retries: 3
```

#### Worker Service (my-job-scraper-worker)
1. Click "Add Service" → "Docker Compose" → "Upload Files"
2. Service Name: `job-scraper-worker`
3. Docker Compose Configuration:
```yaml
version: '3.8'
services:
  worker:
    build:
      context: .
      dockerfile: Dockerfile.force_rebuild_ultimate
    container_name: my-job-scraper-worker
    restart: unless-stopped
    environment:
      POSTGRES_URL: postgres://aromanon:Afmg2486!@my-job-scraper_my-job-scraper:5432/job-data?sslmode=disable
      REDIS_URL: redis://default:Afmg2486!@my-job-scraper_my-job-scraper_redis:6379
      WORKER_ENGINE: primary
      PYTHONPATH: /app:/app/jobspy
    volumes:
      - ./logs:/app/logs
```

### Method 2: Git Repository (Simpler)

1. Click "Add Service" → "Git Repository"
2. Connect your GitHub repository with JobSpy code
3. EasyPanel will auto-detect Python application
4. Use the configuration files we created:
   - `.nixpacks.toml` - Build configuration
   - `Procfile` - Process commands
   - `requirements.txt` - Python dependencies

### Method 3: Direct File Upload with Pre-built Images

If you prefer using pre-built Docker images instead of building in EasyPanel:

1. Click "Add Service" → "Docker Image"
2. Image: Use a pre-built Python image like `python:3.11-slim`
3. Set environment variables and volumes as needed
4. Use the startup command: `python worker_admin.py`

### 3. Configure Environment Variables
For each service, click on "Environment" tab and add:
```
POSTGRES_URL=postgres://aromanon:Afmg2486!@my-job-scraper_my-job-scraper:5432/job-data?sslmode=disable
REDIS_URL=redis://default:Afmg2486!@my-job-scraper_my-job-scraper_redis:6379
SECRET_KEY=your-secret-key-here-show-be-kept-secret-random-string
```

### 4. Deploy Services
1. Click "Deploy" for each service
2. EasyPanel will automatically:
   - Build Docker images
   - Start containers
   - Manage networking
   - Handle health checks
   - Provide logs and monitoring

### 5. Access Services
- **Frontend**: EasyPanel will provide URL (usually http://your-server-ip:5000)
- **Health Check**: http://your-server-ip:5000/health
- **Logs**: Available in EasyPanel dashboard for each service

### 6. Database Initialization
EasyPanel provides in-browser terminal access:
1. Go to frontend service → "Terminal" tab
2. Run: `python -c "from worker_manager import WorkerManager; wm = WorkerManager(); wm.initialize_database()"`
3. Or access the web interface and it will auto-initialize on first use

### 7. Monitoring
- Use EasyPanel's built-in monitoring dashboard
- View container logs through web interface
- Health checks are automatically monitored
- Resource usage is displayed in real-time

### 8. Scaling
To add more workers:
1. Go to worker service
2. Click "Duplicate"
3. Update container name and WORKER_ENGINE environment
4. Deploy new instance

## Why EasyPanel is Better Than Scripts

### No Complex Scripts Needed
- **Before**: 1404-line bash script with complex error handling
- **With EasyPanel**: Simple YAML configuration through web interface

### Built-in Features
- ✅ Automatic SSL certificates
- ✅ Health monitoring and alerts
- ✅ Log aggregation and search
- ✅ Resource usage monitoring
- ✅ One-click deployments
- ✅ Zero-downtime updates
- ✅ Container auto-restart
- ✅ In-browser terminal access
- ✅ Docker Compose support
- ✅ Git integration

### Simplified Management
- **Configuration**: Web interface instead of editing .env files
- **Deployment**: Click "Deploy" instead of running complex scripts
- **Monitoring**: Built-in dashboards instead of manual health checks
- **Scaling**: Duplicate services instead of modifying complex configs
- **Updates**: Push to deploy from Git if needed

## Troubleshooting

### Common Issues in EasyPanel
1. **Service won't start**: Check logs in EasyPanel dashboard
2. **Port conflicts**: EasyPanel will suggest available ports
3. **Database connection**: Verify environment variables in service config
4. **Build failures**: EasyPanel shows build logs with errors

### Getting Help
- EasyPanel has built-in help documentation
- Community support available
- Much simpler than debugging complex bash scripts!

## Next Steps
1. Log into your EasyPanel dashboard
2. Create the two services as described
3. Configure environment variables
4. Deploy and start scraping jobs!

The entire deployment process that took me 1404 lines of script to describe can be done in EasyPanel with just a few clicks through the web interface.