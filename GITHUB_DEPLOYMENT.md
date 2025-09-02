# 🚀 GitHub Deployment Guide for EasyPanel

## Why GitHub Deployment is Better

### ✅ Advantages over File Upload
- **Automatic Builds**: Push code → Auto-deploy
- **Version Control**: Track all changes and rollbacks
- **Team Collaboration**: Multiple developers can contribute
- **CI/CD Pipeline**: Automated testing and deployment
- **No Manual Uploads**: Just git push update
- **Environment Management**: Different branches for dev/staging/production
- **One-Click Rollbacks**: Revert to any previous commit

### ✅ EasyPanel + GitHub Integration
- **Native Support**: EasyPanel has built-in GitHub integration
- **Webhook Triggers**: Automatic deployments on push
- **Build Logs**: Detailed build output in EasyPanel dashboard
- **Environment Variables**: Secure storage separate from code
- **Automatic SSL**: HTTPS certificates managed automatically

## 📋 GitHub Repository Setup

### Step 1: Create GitHub Repository
1. Go to [GitHub](https://github.com) → "New repository"
2. Repository name: `job-scraper` 
3. Set to Private (recommended for scraping projects)
4. Don't initialize with README (we'll add our files)
5. Click "Create repository"

### Step 2: Prepare Local Repository
```bash
# Navigate to your JobSpy directory
cd /path/to/JobSpy

# Initialize git repository
git init

# Add GitHub remote (replace with your repo URL)
git remote add origin https://github.com/YOUR_USERNAME/job-scraper.git

# Add all files
git add .

# Initial commit
git commit -m "Initial commit: Job scraping system with EasyPanel deployment"

# Push to GitHub
git push -u origin main
```

### Step 3: Required Files for GitHub Deployment
Make sure these files are in your repository root:

```
job-scraper/
├── worker_admin.py              # Flask admin interface
├── worker_manager.py            # Worker orchestration  
├── modular_schema.sql           # Database schema
├── Dockerfile.force_rebuild_ultimate          # Main Docker build
├── requirements.txt           # Python dependencies
├── .nixpacks.toml             # EasyPanel build config
├── Procfile                   # Process commands
├── .env.example              # Environment template
├── README.md                 # Documentation
├── EASYPANEL_DEPLOYMENT.md   # Deployment guide
├── DEPLOYMENT_CHECKLIST.md   # Checklist
└── TROUBLESHOOTING_BUILDS.md # Troubleshooting
```

## 🚀 EasyPanel GitHub Deployment

### Option 1: EasyPanel Dashboard (Recommended)

#### Frontend Service Creation
1. **Log into EasyPanel Dashboard**
2. **Click "Add Service" → "Git Repository"**
3. **Repository Configuration:**
   - **Repository URL**: `https://github.com/YOUR_USERNAME/job-scraper.git`
   - **Branch**: `main` (or your default branch)
   - **Name**: `job-scraper-frontend`
   - **Service Type**: EasyPanel should auto-detect "Python"

4. **Build Configuration:**
   - **Dockerfile**: Select `Dockerfile.force_rebuild_ultimate`
   - **Build Context**: `.` (root directory)
   - **Runtime**: Should auto-detect Python 3.11

5. **Environment Variables:**
   ```bash
   POSTGRES_URL=postgres://aromanon:Afmg2486!@my-job-scraper_my-job-scraper:5432/job-data?sslmode=disable
   REDIS_URL=redis://default:Afmg2486!@my-job-scraper_my-job-scraper_redis:6379
   SECRET_KEY=your-secret-key-here-change-this
   FLASK_APP=worker_admin.py
   PYTHONPATH=/app:/app/jobspy
   FLASK_ENV=production
   PYTHONUNBUFFERED=1
   PYTHONDONTWRITEBYTECODE=1
   ```

6. **Port Configuration:**
   - **Container Port**: `5000`
   - **Protocol**: `HTTP`
   - **Health Check**: `/health`

7. **Click "Deploy"**

#### Worker Service Creation
1. **Click "Add Service" → "Git Repository"** again
2. **Repository Configuration:**
   - **Repository URL**: Same as above
   - **Name**: `job-scraper-worker`

3. **Build Configuration:**
   - **Dockerfile**: Select `Dockerfile.force_rebuild_ultimate`
   - **Build Context**: `.` (root directory)

4. **Environment Variables:**
   ```bash
   POSTGRES_URL=postgres://aromanon:Afmg2486!@my-job-scraper_my-job-scraper:5432/job-data?sslmode=disable
   REDIS_URL=redis://default:Afmg2486!@my-job-scraper_my-job-scraper_redis:6379
   WORKER_ENGINE=primary
   PYTHONPATH=/app:/app/jobspy
   PYTHONUNBUFFERED=1
   PYTHONDONTWRITEBYTECODE=1
   ```

5. **Click "Deploy"**

### Option 2: EasyPanel API (Advanced)

```bash
# Create services via EasyPanel API
curl -X POST http://your-easypanel-ip:3000/api/services \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -d '{
    "name": "job-scraper-frontend",
    "source": {
      "type": "git",
      "repository": "https://github.com/YOUR_USERNAME/job-scraper.git",
      "branch": "main"
    },
    "build": {
      "dockerfile": "Dockerfile.force_rebuild_ultimate"
    },
    "environment": {
      "POSTGRES_URL": "your-db-url",
      "REDIS_URL": "your-redis-url",
      "SECRET_KEY": "your-secret-key"
    }
  }'
```

### Option 3: EasyPanel CLI (If Available)

```bash
# Connect to your VPS and use EasyPanel CLI
easypanel service create \
  --name job-scraper-frontend \
  --source git \
  --repository https://github.com/YOUR_USERNAME/job-scraper.git \
  --branch main \
  --dockerfile Dockerfile.force_rebuild_ultimate

# Set environment variables
easypanel service env set \
  --name job-scraper-frontend \
  --env POSTGRES_URL=your-db-url \
  --env REDIS_URL=your-redis-url
```

## ⚡ Automatic Deployment on Push

### Enable Webhooks (Automatic Updates)
1. In EasyPanel service dashboard → "Settings" → "Webhooks"
2. Enable "Auto-deploy on push"
3. Copy webhook URL
4. Go to GitHub repository → "Settings" → "Webhooks" → "Add webhook"
5. Paste webhook URL and select "Just the push event"
6. Add webhook

Now every push to main branch triggers automatic deployment!

## 🔄 Multi-Environment Deployment

### Production Branch Strategy
```bash
# Production branch (main)
git checkout main
git push origin main  # → Auto-deploys to production

# Development branch
git checkout -b develop
git push origin develop  # → Deploy to staging environment

# Feature branches
git checkout -b feature/new-scrapers
git push origin feature/new-scrapers  # → Deploy to preview environment
```

### Environment-Specific Configuration
Create different EasyPanel services for different branches:

| Service Name | Branch | Environment | Purpose |
|--------------|--------|-------------|---------|
| `job-scraper-prod` | `main` | Production | Live scraping |
| `job-scraper-staging` | `develop` | Staging | Testing new features |
| `job-scraper-feature` | `feature/*` | Preview | Feature testing |

## 🔐 Secret Management

### Recommended: Use EasyPanel Environment Variables
Never commit secrets to GitHub! Use EasyPanel's secure environment storage:

```bash
# In EasyPanel dashboard → Service → Environment tab
POSTGRES_URL=your-production-db-url
REDIS_URL=your-production-redis-url
SECRET_KEY=your-production-secret-key
```

### Alternative: GitHub Secrets (For CI/CD)
If you use GitHub Actions, store secrets in repository settings:

1. GitHub: **Repository → Settings → Secrets and variables → Actions**
2. Add production secrets
3. Reference them in workflows

## 📊 Monitoring GitHub Deployments

### EasyPanel Dashboard Features
- **Deployment History**: View all previous deployments
- **Build Logs**: Real-time build output and errors
- **Health Monitoring**: Service health and resource usage
- **Log Aggregation**: Centralized logs across services
- **Performance Metrics**: CPU, memory, disk usage

### GitHub Integration Benefits
- **Commit Links**: Direct links from commits to deployments
- **Branch Tracking**: See which branch is deployed
- **Rollback Options**: One-click revert to previous commit
- **Deployment Status**: GitHub checks show deployment status

## 🚨 Troubleshooting GitHub Deployment

### Common Issues & Solutions

#### Issue 1: Build Fails on EasyPanel
**Symptoms**: Build process fails with Nixpacks error
**Fix**: Ensure `.nixpacks.toml` and `requirements.txt` are committed

#### Issue 2: Repository Not Found
**Symptoms**: Error accessing GitHub repository
**Fix**: 
- Check repository URL in EasyPanel
- Ensure repository is public or use GitHub access token
- Verify repository exists

#### Issue 3: Environment Variables Missing
**Symptoms**: Application starts but can't connect to database
**Fix**: Add environment variables in EasyPanel service settings

#### Issue 4: Docker Build Context Issues
**Symptoms**: Docker can't find files during build
**Fix**: Ensure build context is set to `.` and all files are committed to git

### Quick Commands for Testing

```bash
# Test local build first
docker build -t test-job-scraper -f Dockerfile.force_rebuild_ultimate .

# Check repository accessibility
curl -I https://github.com/YOUR_USERNAME/job-scraper.git

# Test environment variables (via EasyPanel terminal)
echo $POSTGRES_URL
echo $REDIS_URL
```

## 🎯 Best Practices

### Repository Structure
```
job-scraper/
├── .gitignore                 # Exclude temporary files
├── README.md                  # Project documentation
├── requirements.txt          # Python dependencies
├── .nixpacks.toml           # Build configuration
├── Procfile                  # Process commands
├── Dockerfile.force_rebuild_ultimate       # Main service
├── src/                     # Source code
│   ├── worker_admin.py
│   ├── worker_manager.py
│   └── modular_schema.sql
├── configs/                  # Configuration files
├── docs/                     # Documentation
└── scripts/                  # Utility scripts
```

### `.gitignore` File
```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
.env
.venv

# Docker
.dockerignore

# Logs
*.log
logs/

# Temporary files
*.tmp
*.temp
.DS_Store
Thumbs.db

# EasyPanel specific
.env.local
.env.production
.env.staging

# Database backups
*.backup
*.sql.dump
```

### Commit Guidelines
```bash
# Semantic commit messages
feat: add linkedin scraper support
fix: resolve database connection timeout
docs: update deployment documentation
style: format code and improve readability
refactor: improve worker manager performance
test: add integration tests for database connection
chore: update dependencies in requirements.txt
```

---

## 🚀 Ready to Deploy via GitHub?

**Summary of Actions:**
1. ✅ Create GitHub repository
2. ✅ Commit all JobSpy files with proper structure
3. ✅ Use EasyPanel "Git Repository" deployment method
4. ✅ Configure environment variables in EasyPanel
5. ✅ Enable webhooks for automatic updates

**Result**: Push code → Auto-deploy to production with full version control! 🎉