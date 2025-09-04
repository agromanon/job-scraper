#!/bin/bash
set -e

echo "=== Job Scraper Startup Script ==="
echo "Current directory: $(pwd)"
echo "Files in /app: $(ls -la /app 2>/dev/null || echo 'Cannot list /app')"

# Run database fix script
echo "Running database fix script..."
cd /app && python3 fix_database.py

# Start the main application
echo "Starting job scraper application..."
exec python worker_admin.py