# 🐛 EasyPanel Build Error Troubleshooting

## Error: Command failed with exit code 1 in Nixpacks build

This error occurs when EasyPanel's Nixpacks build system can't automatically detect how to build your Python application. Here are the solutions:

## ✅ Quick Fix: Try These Methods in Order

### Method 1: Use the Configuration Files I Created
I've already created the files that should fix this issue. Make sure these files are uploaded:

1. **.nixpacks.toml** - Tells Nixpacks exactly how to build
2. **requirements.txt** - All Python dependencies
3. **Procfile** - Process commands for starting the app
4. **Dockerfile.frontend** - Docker build instructions

### Method 2: Use Docker Compose Instead

1. In EasyPanel, click "Add Service" → "Docker Compose" instead of "Git Repository"
2. Use the exact YAML configuration from `EASYPANEL_DEPLOYMENT.md`
3. This bypasses Nixpacks and uses Docker directly

### Method 3: Use Pre-built Docker Image

1. Click "Add Service" → "Docker Image"
2. Use image: `python:3.11-slim`
3. Set these values:
   - **Command**: `python worker_admin.py`
   - **Working Directory**: `/app`
   - **Environment Variables**: Add the database URLs and PYTHONPATH

### Method 4: Fix Build Dependencies

The error might be due to missing system dependencies. Update your build approach:

#### Option A: Use a different Python version
Try changing from `python:3.11-slim` to `python:3.11` (full image includes more system packages)

#### Option B: Install System Packages
In your Dockerfile or build configuration, ensure these are installed:
```dockerfile
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*
```

## 🔧 Detailed Troubleshooting Steps

### Step 1: Check Build Logs
In EasyPanel, click on your service and look at the "Build Logs" tab. The exact error message will tell us what's failing.

### Step 2: Verify File Structure
Make sure you have all required files in the root directory:
```
JobSpy/
├── worker_admin.py          # Main Flask app
├── worker_manager.py        # Worker manager
├── modular_schema.sql       # Database schema
├── Dockerfile.frontend      # Docker build instructions
├── Dockerfile.worker        # Worker Docker file
├── .nixpacks.toml          # Build configuration (NEW)
├── requirements.txt        # Python dependencies (NEW)
├── Procfile                # Process commands (NEW)
├── .                    # (Empty file to mark directory)
```

### Step 3: Test Dependencies
Create a test file to verify dependencies:
```python
# test_dependencies.py
print("Testing imports...")

try:
    import flask
    print("✓ Flask")
except:
    print("✗ Flask missing")

try:
    import psycopg2
    print("✓ psycopg2")
except:
    print("✗ psycopg2 missing")

try:
    import redis
    print("✓ Redis")
except:
    print("✗ Redis missing")

try:
    import jobspy
    print("✓ JobSpy")
except:
    print("✗ JobSpy missing")
```

Upload this file and see if it runs in the EasyPanel terminal.

### Step 4: Alternative Deployment Commands

If the GUI fails, try using EasyPanel's API or CLI:

#### Using EasyPanel CLI (if available):
```bash
# Connect to EasyPanel SSH or terminal
easypanel service create --name job-scraper-frontend --source dockerfile --dockerfile Dockerfile.frontend
easypanel service set-env --name job-scraper-frontend --env POSTGRES_URL=your-db-url
```

#### Using cURL API:
```bash
# Create service via EasyPanel API
curl -X POST http://your-easypanel-ip:3000/api/services \
  -H "Content-Type: application/json" \
  -d '{
    "name": "job-scraper-frontend",
    "image": "python:3.11-slim",
    "command": "python worker_admin.py",
    "env": {
      "POSTGRES_URL": "your-db-url",
      "PYTHONPATH": "/app:/app/jobspy"
    }
  }'
```

## 🔄 Service-Level Fix

If the frontend service keeps failing, create the worker service first, then access it via terminal to install dependencies:

1. Create worker service first
2. Go to worker service → "Terminal" tab
3. Run manual installation:
```bash
apt-get update
apt-get install -y gcc g++ libpq-dev git curl
pip install flask psycopg2-binary redis requests python-dotenv
pip install git+https://github.com/cullenwatson/JobSpy.git@master
```
4. Then create the frontend service

## 📝 Common Nixpacks Issues and Solutions

### Issue 1: Missing Runtime Detection
**Problem**: Nixpacks can't detect this is a Python app
**Solution**: Add `.nixpacks.toml` file with explicit Python runtime

### Issue 2: Build Dependencies Missing  
**Problem**: PostgreSQL and other system packages missing
**Solution**: Include build deps in `.nixpacks.toml` or use full Python image

### Issue 3: JobPy Installation Fails
**Problem**: Complex Git-based installation fails
**Solution**: Install JobSpy after基础 dependencies or use Docker Compose

### Issue 4: Circular Dependencies
**Problem**: Some packages require system libraries
**Solution**: Use full Python image or install system packages first

## 🚀 Backup Plan: Manual Deployment

If EasyPanel keeps failing, you can deploy manually via SSH:

```bash
# SSH to your VPS
ssh root@your-server-ip

# Navigate to project
cd /path/to/JobSpy

# Manual Docker build and run
docker build -t job-scraper-frontend -f Dockerfile.frontend .
docker run -d \
  --name my-job-scraper-fe \
  -p 5000:5000 \
  -e POSTGRES_URL="your-db-url" \
  -e REDIS_URL="your-redis-url" \
  job-scraper-frontend
```

## 📞 Getting Help

### Next Steps:
1. **Check build logs** first - they tell exactly what failed
2. **Try Docker Compose method** - more reliable than Nixpacks
3. **Use pre-built image** - if build keeps failing
4. **Contact EasyPanel support** - provides build logs and error details

### What to Report:
- Exact error message from build logs
- Which method you tried (Docker Compose, Git, Docker Image)
- All files you uploaded
- Your EasyPanel version

---

**Remember**: The easiest solution is usually Method 2 (Docker Compose) or Method 3 (Pre-built Image). These bypass the Nixpacks auto-detection that's causing the error.